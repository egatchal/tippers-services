# backend/api/routes/models.py
"""ML model upload + registration endpoints (MLflow-first)."""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import List, Optional
from backend.db.session import get_db
from backend.db.models import HostedModel, HostedModelVersion

import os
import json
import zipfile
import tempfile
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient

router = APIRouter()


# ---- Helpers ----

MAX_ZIP_BYTES = 500 * 1024 * 1024  # 500MB MVP limit; tune later


def _safe_extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """Prevent Zip Slip path traversal."""
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.infolist():
            member_path = (dest_dir / member.filename).resolve()
            if not str(member_path).startswith(str(dest_dir.resolve())):
                raise HTTPException(status_code=400, detail="Invalid zip: unsafe paths detected.")
        z.extractall(dest_dir)


def _normalize_model_dir(extracted_dir: Path) -> Path:
    """
    Normalize extracted content to a single model directory that contains MLmodel.
    Accept either:
      - zip root contains MLmodel
      - zip root contains a single folder that contains MLmodel
    """
    # Case 1: MLmodel at root
    if (extracted_dir / "MLmodel").exists():
        return extracted_dir

    # Case 2: single top-level directory
    children = [p for p in extracted_dir.iterdir() if p.name not in [".DS_Store", "__MACOSX"]]
    dirs = [p for p in children if p.is_dir() and p.name != "__MACOSX"]
    files = [p for p in children if p.is_file()]

    if files:
        # root contains files but no MLmodel => reject
        raise HTTPException(status_code=400, detail="Zip must contain an MLflow model directory (missing MLmodel).")

    if len(dirs) == 1 and (dirs[0] / "MLmodel").exists():
        return dirs[0]

    # Otherwise ambiguous layout
    raise HTTPException(
        status_code=400,
        detail="Zip must contain exactly one MLflow model directory with an MLmodel file."
    )


def _ensure_mlflow_tracking_uri():
    """
    Ensure your MLflow tracking URI is configured.
    In k8s you'll typically set MLFLOW_TRACKING_URI env var.
    """
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise HTTPException(
            status_code=500,
            detail="MLFLOW_TRACKING_URI is not configured on the server."
        )
    mlflow.set_tracking_uri(tracking_uri)


# ---- Routes ----

@router.post(
    "",
    status_code=status.HTTP_201_CREATED
)
async def upload_and_register_model(
    file: UploadFile = File(...),
    name: str = Form(...),
    activity: str = Form("occupancy"),
    description: Optional[str] = Form(None),
    visibility: str = Form("private"),
    tags_json: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """
    Upload an MLflow model bundle zip and register it into MLflow Model Registry.

    This endpoint:
    1. Validates zip structure & presence of MLmodel
    2. Creates an MLflow run and logs the model directory as artifacts
    3. Registers a new MLflow model version under `name`
    4. Writes/updates minimal rows in Postgres (HostedModel, HostedModelVersion)

    Form fields:
    - file: zip containing an MLflow model directory (must include MLmodel)
    - name: registered model name (and your logical name)
    - activity: e.g., occupancy
    - description: optional
    - visibility: private/public (MVP metadata)
    - tags_json: optional JSON string of tags to set on MLflow model version
    """
    _ensure_mlflow_tracking_uri()

    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Upload must be a .zip containing an MLflow model directory.")

    # Parse optional tags
    tags = {}
    if tags_json:
        try:
            tags = json.loads(tags_json)
            if not isinstance(tags, dict):
                raise ValueError("tags_json must be a JSON object")
        except Exception:
            raise HTTPException(status_code=400, detail="tags_json must be a valid JSON object.")

    # MVP owner (replace with your auth principal later)
    owner_id = "default"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)
        zip_path = tmp_dir / "upload.zip"

        # Save upload with size limit
        total = 0
        with open(zip_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_ZIP_BYTES:
                    raise HTTPException(status_code=413, detail="Upload too large.")
                out.write(chunk)

        # Extract safely
        extracted_dir = tmp_dir / "extracted"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        _safe_extract_zip(zip_path, extracted_dir)

        model_dir = _normalize_model_dir(extracted_dir)

        # Create or update HostedModel row
        hosted_model = db.query(HostedModel).filter(
            HostedModel.owner_id == owner_id,
            HostedModel.name == name
        ).first()

        if not hosted_model:
            hosted_model = HostedModel(
                owner_id=owner_id,
                name=name,
                description=description,
                activity=activity,
                visibility=visibility,
                mlflow_registered_name=name,
                status="ACTIVE"
            )
            db.add(hosted_model)
            db.commit()
            db.refresh(hosted_model)
        else:
            # Update metadata if provided
            if description is not None:
                hosted_model.description = description
            hosted_model.activity = activity
            hosted_model.visibility = visibility
            hosted_model.mlflow_registered_name = name
            db.commit()

        # Register in MLflow: create a run, log artifacts, then register model version
        client = MlflowClient()

        with mlflow.start_run() as run:
            run_id = run.info.run_id

            # Log the model directory contents to artifact path "model"
            mlflow.log_artifacts(str(model_dir), artifact_path="model")

            model_uri = f"runs:/{run_id}/model"

        # Register creates a new model version in MLflow Registry
        try:
            mv = mlflow.register_model(model_uri=model_uri, name=name)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MLflow registration failed: {str(e)}")

        # Tag the model version (MLflow-first metadata)
        # Store your activity/visibility + any user tags
        version_tags = {
            "activity": activity,
            "visibility": visibility,
            "owner_id": owner_id,
            **{str(k): str(v) for k, v in tags.items()},
        }
        for k, v in version_tags.items():
            try:
                client.set_model_version_tag(name=name, version=mv.version, key=k, value=v)
            except Exception:
                # Tags aren't critical for MVP success
                pass

        # Persist HostedModelVersion row
        version_row = HostedModelVersion(
            model_id=hosted_model.model_id,
            mlflow_version=int(mv.version),
            mlflow_run_id=run_id,
            artifact_uri=model_uri,
            stage=None,
            status="REGISTERED"
        )
        db.add(version_row)
        db.commit()
        db.refresh(version_row)

        return {
            "model_id": hosted_model.model_id,
            "name": hosted_model.name,
            "activity": hosted_model.activity,
            "visibility": hosted_model.visibility,
            "mlflow_registered_name": hosted_model.mlflow_registered_name,
            "mlflow_version": version_row.mlflow_version,
            "run_id": run_id,
            "artifact_uri": model_uri,
            "status": version_row.status
        }


@router.get("")
async def list_models(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List models from the app DB (MVP catalog).
    """
    models = db.query(HostedModel).order_by(HostedModel.created_at.desc()).offset(skip).limit(limit).all()

    return [
        {
            "model_id": m.model_id,
            "name": m.name,
            "activity": m.activity,
            "visibility": m.visibility,
            "status": m.status,
            "mlflow_registered_name": m.mlflow_registered_name,
            "created_at": m.created_at,
            "updated_at": m.updated_at
        }
        for m in models
    ]


@router.get("/{model_id}")
async def get_model(
    model_id: int,
    db: Session = Depends(get_db)
):
    """
    Get model details + versions from the app DB.
    """
    m = db.query(HostedModel).filter(HostedModel.model_id == model_id).first()
    if not m:
        raise HTTPException(status_code=404, detail=f"Model with ID {model_id} not found")

    versions = db.query(HostedModelVersion).filter(
        HostedModelVersion.model_id == model_id
    ).order_by(HostedModelVersion.created_at.desc()).all()

    return {
        "model_id": m.model_id,
        "name": m.name,
        "activity": m.activity,
        "visibility": m.visibility,
        "status": m.status,
        "description": m.description,
        "mlflow_registered_name": m.mlflow_registered_name,
        "created_at": m.created_at,
        "updated_at": m.updated_at,
        "versions": [
            {
                "model_version_id": v.model_version_id,
                "mlflow_version": v.mlflow_version,
                "mlflow_run_id": v.mlflow_run_id,
                "artifact_uri": v.artifact_uri,
                "stage": v.stage,
                "status": v.status,
                "created_at": v.created_at
            }
            for v in versions
        ]
    }


@router.get("/{model_id}/versions")
async def list_model_versions(
    model_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List versions for a given model (from app DB).
    """
    m = db.query(HostedModel).filter(HostedModel.model_id == model_id).first()
    if not m:
        raise HTTPException(status_code=404, detail=f"Model with ID {model_id} not found")

    versions = db.query(HostedModelVersion).filter(
        HostedModelVersion.model_id == model_id
    ).order_by(HostedModelVersion.created_at.desc()).offset(skip).limit(limit).all()

    return [
        {
            "model_version_id": v.model_version_id,
            "mlflow_version": v.mlflow_version,
            "mlflow_run_id": v.mlflow_run_id,
            "artifact_uri": v.artifact_uri,
            "stage": v.stage,
            "status": v.status,
            "created_at": v.created_at
        }
        for v in versions
    ]
