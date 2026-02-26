"""Model serving — deploy, list, and predict endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional
from backend.db.session import get_db
from backend.db.models import ServiceDeployment, HostedModel, HostedModelVersion
from backend.schemas import (
    DeploymentCreate,
    DeploymentResponse,
    PredictRequest,
    PredictResponse,
)

router = APIRouter()


# ── Deployments ──────────────────────────────────────────────────────────────

@router.post("/deployments", response_model=DeploymentResponse, status_code=status.HTTP_201_CREATED)
async def create_deployment(request: DeploymentCreate, db: Session = Depends(get_db)):
    """Deploy a model version to a service."""
    # Validate model and version exist
    model = db.query(HostedModel).filter(HostedModel.model_id == request.model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail=f"Model {request.model_id} not found")

    version = db.query(HostedModelVersion).filter(
        HostedModelVersion.model_version_id == request.model_version_id,
        HostedModelVersion.model_id == request.model_id,
    ).first()
    if not version:
        raise HTTPException(status_code=404, detail=f"Model version {request.model_version_id} not found")

    # Build model URI if not provided
    model_uri = request.mlflow_model_uri
    if not model_uri:
        model_uri = f"models:/{model.mlflow_registered_name}/{version.mlflow_version}"

    # Deactivate existing deployment for this (service, model_id)
    existing = db.query(ServiceDeployment).filter(
        ServiceDeployment.service == request.service,
        ServiceDeployment.model_id == request.model_id,
        ServiceDeployment.status == 'ACTIVE',
    ).first()
    if existing:
        existing.status = 'INACTIVE'

    dep = ServiceDeployment(
        model_id=request.model_id,
        model_version_id=request.model_version_id,
        service=request.service,
        deploy_config=request.deploy_config,
        mlflow_model_uri=model_uri,
    )
    db.add(dep)
    db.commit()
    db.refresh(dep)

    # Clear model cache so next predict loads updated model
    from backend.utils.model_loader import clear_model_cache
    clear_model_cache()

    return dep


@router.get("/deployments", response_model=List[DeploymentResponse])
async def list_deployments(
    service: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List deployments, optionally filtered by service."""
    query = db.query(ServiceDeployment)
    if service:
        query = query.filter(ServiceDeployment.service == service)
    return query.order_by(ServiceDeployment.created_at.desc()).offset(skip).limit(limit).all()


@router.get("/deployments/{deployment_id}", response_model=DeploymentResponse)
async def get_deployment(deployment_id: int, db: Session = Depends(get_db)):
    """Get deployment details."""
    dep = db.query(ServiceDeployment).filter(
        ServiceDeployment.deployment_id == deployment_id
    ).first()
    if not dep:
        raise HTTPException(status_code=404, detail=f"Deployment {deployment_id} not found")
    return dep


@router.delete("/deployments/{deployment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deactivate_deployment(deployment_id: int, db: Session = Depends(get_db)):
    """Deactivate a deployment."""
    dep = db.query(ServiceDeployment).filter(
        ServiceDeployment.deployment_id == deployment_id
    ).first()
    if not dep:
        raise HTTPException(status_code=404, detail=f"Deployment {deployment_id} not found")
    dep.status = 'INACTIVE'
    db.commit()
    return None


# ── Prediction ───────────────────────────────────────────────────────────────

@router.post("/predict/{deployment_id}", response_model=PredictResponse)
async def predict(
    deployment_id: int,
    request: PredictRequest,
    db: Session = Depends(get_db),
):
    """Real-time inference using a specific deployment."""
    dep = db.query(ServiceDeployment).filter(
        ServiceDeployment.deployment_id == deployment_id,
        ServiceDeployment.status == 'ACTIVE',
    ).first()
    if not dep:
        raise HTTPException(status_code=404, detail=f"Active deployment {deployment_id} not found")

    return _run_prediction(dep, request, db)


@router.post("/predict-by-service/{service}", response_model=PredictResponse)
async def predict_by_service(
    service: str,
    request: PredictRequest,
    db: Session = Depends(get_db),
):
    """Infer using the active deployment for a given service."""
    dep = db.query(ServiceDeployment).filter(
        ServiceDeployment.service == service,
        ServiceDeployment.status == 'ACTIVE',
    ).order_by(ServiceDeployment.created_at.desc()).first()

    if not dep:
        raise HTTPException(status_code=404, detail=f"No active deployment for service '{service}'")

    return _run_prediction(dep, request, db)


def _run_prediction(dep: ServiceDeployment, request: PredictRequest, db: Session) -> PredictResponse:
    """Internal helper: load model, run prediction, return response."""
    import pandas as pd
    from backend.utils.model_loader import load_cached_model

    model_uri = dep.mlflow_model_uri
    if not model_uri:
        raise HTTPException(status_code=400, detail="Deployment has no mlflow_model_uri")

    try:
        model = load_cached_model(model_uri)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load model: {e}")

    df = pd.DataFrame(request.instances)

    try:
        raw = model.predict(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

    predictions = raw.tolist() if hasattr(raw, 'tolist') else list(raw)

    # Try to get probabilities if model supports predict_proba
    prediction_probs = None
    try:
        if hasattr(model, '_model_impl') and hasattr(model._model_impl, 'predict_proba'):
            probs = model._model_impl.predict_proba(df)
            prediction_probs = probs.tolist() if hasattr(probs, 'tolist') else [list(p) for p in probs]
    except Exception:
        pass

    # Look up model metadata for the response
    model_obj = db.query(HostedModel).filter(HostedModel.model_id == dep.model_id).first()
    version_obj = db.query(HostedModelVersion).filter(
        HostedModelVersion.model_version_id == dep.model_version_id
    ).first()

    return PredictResponse(
        deployment_id=dep.deployment_id,
        model_name=model_obj.name if model_obj else "unknown",
        model_version=version_obj.mlflow_version if version_obj else 0,
        predictions=predictions,
        prediction_probs=prediction_probs,
    )
