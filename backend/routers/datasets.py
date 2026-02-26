"""Unified dataset catalog endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import text as sa_text
from typing import List, Optional
from backend.db.session import get_db
from backend.db.models import Dataset
from backend.schemas import DatasetCreate, DatasetResponse, DatasetPreviewResponse, BatchInferenceRequest
import os

router = APIRouter()


@router.get("", response_model=List[DatasetResponse])
async def list_datasets(
    service: Optional[str] = None,
    dataset_type: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List datasets with optional filters."""
    query = db.query(Dataset).filter(Dataset.status != 'DELETED')
    if service:
        query = query.filter(Dataset.service == service)
    if dataset_type:
        query = query.filter(Dataset.dataset_type == dataset_type)
    if status_filter:
        query = query.filter(Dataset.status == status_filter)
    return query.order_by(Dataset.created_at.desc()).offset(skip).limit(limit).all()


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Get a specific dataset by ID."""
    ds = db.query(Dataset).filter(Dataset.dataset_id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    return ds


@router.get("/{dataset_id}/preview", response_model=DatasetPreviewResponse)
async def preview_dataset(
    dataset_id: int,
    n: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Preview the first N rows of a parquet dataset."""
    ds = db.query(Dataset).filter(Dataset.dataset_id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    if not ds.storage_path:
        raise HTTPException(status_code=404, detail="Dataset has no storage_path")

    import pandas as pd

    try:
        if ds.storage_path.startswith("s3://"):
            import boto3
            parts = ds.storage_path.replace("s3://", "").split("/", 1)
            s3_bucket, s3_key = parts[0], parts[1]
            local_path = f"/tmp/preview_dataset_{dataset_id}.parquet"
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT_URL"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            )
            s3_client.download_file(s3_bucket, s3_key, local_path)
            df = pd.read_parquet(local_path)
        else:
            df = pd.read_parquet(ds.storage_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read dataset: {e}")

    rows = df.head(n).to_dict(orient="records")
    return DatasetPreviewResponse(
        dataset_id=dataset_id,
        row_count=len(rows),
        columns=list(df.columns),
        rows=rows,
    )


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def register_dataset(request: DatasetCreate, db: Session = Depends(get_db)):
    """Manually register an external dataset."""
    ds = Dataset(
        name=request.name,
        description=request.description,
        service=request.service,
        dataset_type=request.dataset_type,
        format=request.format,
        storage_path=request.storage_path,
        source_ref=request.source_ref,
        row_count=request.row_count,
        column_stats=request.column_stats,
        schema_info=request.schema_info,
        tags=request.tags,
    )
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def soft_delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Soft-delete a dataset (sets status to DELETED)."""
    ds = db.query(Dataset).filter(Dataset.dataset_id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    ds.status = "DELETED"
    db.commit()
    return None


@router.post("/{dataset_id}/run-model")
async def run_model_on_dataset(
    dataset_id: int,
    request: BatchInferenceRequest,
    db: Session = Depends(get_db),
):
    """Run a deployed model against a dataset for batch inference."""
    from backend.db.models import ServiceDeployment, HostedModel, HostedModelVersion
    from backend.utils.model_loader import load_cached_model
    from backend.utils.dataset_registry import register_dataset as reg_ds
    import pandas as pd

    ds = db.query(Dataset).filter(Dataset.dataset_id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    if not ds.storage_path:
        raise HTTPException(status_code=400, detail="Dataset has no storage_path")

    deployment = db.query(ServiceDeployment).filter(
        ServiceDeployment.deployment_id == request.deployment_id,
        ServiceDeployment.status == 'ACTIVE',
    ).first()
    if not deployment:
        raise HTTPException(status_code=404, detail=f"Active deployment {request.deployment_id} not found")

    model_uri = deployment.mlflow_model_uri
    if not model_uri:
        raise HTTPException(status_code=400, detail="Deployment has no mlflow_model_uri")

    # Load dataset
    try:
        if ds.storage_path.startswith("s3://"):
            import boto3
            parts = ds.storage_path.replace("s3://", "").split("/", 1)
            local_path = f"/tmp/batch_input_{dataset_id}.parquet"
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT_URL"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            )
            s3_client.download_file(parts[0], parts[1], local_path)
            df = pd.read_parquet(local_path)
        else:
            df = pd.read_parquet(ds.storage_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read dataset: {e}")

    # Run model
    try:
        model = load_cached_model(model_uri)
        predictions = model.predict(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model prediction failed: {e}")

    # Save output
    df_out = df.copy()
    df_out["prediction"] = predictions.tolist() if hasattr(predictions, 'tolist') else list(predictions)

    output_name = request.output_name or f"batch_predictions_ds{dataset_id}_dep{deployment.deployment_id}"
    output_path = f"/tmp/{output_name}.parquet"
    df_out.to_parquet(output_path, index=False)

    # Try uploading to S3
    storage_path = output_path
    try:
        import boto3
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        s3_bucket = os.getenv("S3_BUCKET", "tippers-data")
        s3_key = f"predictions/{output_name}.parquet"
        s3_client.upload_file(output_path, s3_bucket, s3_key)
        storage_path = f"s3://{s3_bucket}/{s3_key}"
    except Exception:
        pass  # Fall back to local

    # Register output dataset
    output_ds_id = reg_ds(
        db,
        name=output_name,
        service=deployment.service,
        dataset_type="predictions",
        storage_path=storage_path,
        row_count=len(df_out),
        source_ref={"input_dataset_id": dataset_id, "deployment_id": deployment.deployment_id},
        tags={"input_dataset_id": dataset_id},
    )

    return {
        "output_dataset_id": output_ds_id,
        "row_count": len(df_out),
        "storage_path": storage_path,
    }
