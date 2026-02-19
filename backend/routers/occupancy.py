"""Occupancy dataset endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import OccupancyDataset
from backend.schemas import OccupancyDatasetCreate, OccupancyDatasetResponse
import json
import os

router = APIRouter()


@router.post("/datasets", response_model=OccupancyDatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_occupancy_dataset(
    request: OccupancyDatasetCreate,
    db: Session = Depends(get_db)
):
    """
    Create and trigger an occupancy dataset computation job.

    Validates inputs, creates a DB record, and submits a Dagster job to compute
    space occupancy bins from WiFi session data.
    """
    # Validate time range
    if request.end_time <= request.start_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_time must be after start_time"
        )

    if request.interval_seconds < 60:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="interval_seconds must be at least 60"
        )

    duration_seconds = (request.end_time - request.start_time).total_seconds()
    max_bins = duration_seconds / request.interval_seconds
    if max_bins > 50000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Too many bins ({int(max_bins)}). Reduce the time range or increase interval_seconds to stay under 50,000 bins."
        )

    # Create DB record
    dataset = OccupancyDataset(
        name=request.name,
        description=request.description,
        root_space_id=request.root_space_id,
        start_time=request.start_time,
        end_time=request.end_time,
        interval_seconds=request.interval_seconds,
        chunk_days=request.chunk_days,
        status="PENDING"
    )

    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    # Submit Dagster job
    from backend.utils.dagster_client import get_dagster_client

    run_config = {
        "ops": {
            "occupancy_dataset": {
                "config": {"dataset_id": dataset.dataset_id}
            }
        }
    }

    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="occupancy_dataset_job",
        run_config=run_config
    )

    dataset.dagster_run_id = result["run_id"]
    dataset.status = "RUNNING"
    db.commit()
    db.refresh(dataset)

    return dataset


@router.get("/datasets", response_model=List[OccupancyDatasetResponse])
async def list_occupancy_datasets(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List all occupancy datasets."""
    datasets = db.query(OccupancyDataset).order_by(
        OccupancyDataset.created_at.desc()
    ).offset(skip).limit(limit).all()
    return datasets


@router.get("/datasets/{dataset_id}", response_model=OccupancyDatasetResponse)
async def get_occupancy_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific occupancy dataset by ID."""
    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    return dataset


@router.get("/datasets/{dataset_id}/results")
async def get_occupancy_dataset_results(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get occupancy dataset results (first 500 rows).

    Returns rows with columns: space_id, interval_begin_time, number_connections.
    Only available once status is COMPLETED.
    """
    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    if dataset.status != "COMPLETED":
        return {
            "dataset_id": dataset_id,
            "status": dataset.status,
            "message": f"Dataset is {dataset.status}. Results not available yet."
        }

    if not dataset.storage_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset completed but storage path not found"
        )

    # Load Parquet from S3 or local path
    import pandas as pd

    storage_path = dataset.storage_path

    if storage_path.startswith("s3://"):
        import boto3
        s3_path_parts = storage_path.replace("s3://", "").split("/", 1)
        s3_bucket = s3_path_parts[0]
        s3_key = s3_path_parts[1]
        local_path = f"/tmp/occupancy_{dataset_id}.parquet"

        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        s3_client.download_file(s3_bucket, s3_key, local_path)
        df = pd.read_parquet(local_path)
    else:
        if not os.path.exists(storage_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Results file not found at {storage_path}"
            )
        df = pd.read_parquet(storage_path)

    rows = df.head(500).to_dict(orient="records")

    return {
        "dataset_id": dataset_id,
        "status": dataset.status,
        "row_count": dataset.row_count,
        "rows": rows,
    }


@router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_occupancy_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """Delete an occupancy dataset record."""
    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    db.delete(dataset)
    db.commit()
