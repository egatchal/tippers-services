"""Classifier training endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import ClassifierJob, SnorkelJob, ConceptFeature
from backend.schemas import (
    ClassifierRunRequest,
    ClassifierJobResponse
)
from datetime import datetime
import json
import os

router = APIRouter()


@router.post("/{c_id}/classifiers/run", response_model=ClassifierJobResponse, status_code=status.HTTP_201_CREATED)
async def run_classifier_training(
    c_id: int,
    request: ClassifierRunRequest,
    db: Session = Depends(get_db)
):
    """
    Trigger classifier training with Snorkel labels and features.

    This endpoint:
    1. Validates the Snorkel job is completed
    2. Validates all features are materialized
    3. Creates a classifier job record
    4. Triggers Dagster training pipeline

    - **c_id**: Concept ID
    - **snorkel_job_id**: Snorkel job whose labels to use
    - **feature_ids**: Feature IDs to join as training data
    - **config**: Classifier training configuration
    """
    # Validate Snorkel job exists and is completed
    snorkel_job = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id,
        SnorkelJob.job_id == request.snorkel_job_id
    ).first()

    if not snorkel_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Snorkel job with ID {request.snorkel_job_id} not found for concept {c_id}"
        )

    if snorkel_job.status != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Snorkel job {request.snorkel_job_id} is not completed (status: {snorkel_job.status})"
        )

    # Validate all features exist and are materialized
    features = db.query(ConceptFeature).filter(
        ConceptFeature.c_id == c_id,
        ConceptFeature.feature_id.in_(request.feature_ids)
    ).all()

    if len(features) != len(request.feature_ids):
        found_ids = {f.feature_id for f in features}
        missing = set(request.feature_ids) - found_ids
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature IDs not found for concept {c_id}: {sorted(missing)}"
        )

    for feature in features:
        if not feature.is_materialized:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Feature '{feature.name}' (ID {feature.feature_id}) is not materialized"
            )

    # Validate threshold_method
    if request.config.threshold_method not in ["entropy", "max_confidence"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="threshold_method must be 'entropy' or 'max_confidence'"
        )

    # Create classifier job
    job = ClassifierJob(
        c_id=c_id,
        snorkel_job_id=request.snorkel_job_id,
        feature_ids=request.feature_ids,
        config=request.config.model_dump(),
        status="PENDING"
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    # Trigger Dagster pipeline
    from backend.utils.dagster_client import get_dagster_client

    run_config = {
        "ops": {
            "classifier_training": {
                "config": {
                    "job_id": job.job_id
                }
            }
        }
    }

    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="classifier_training_pipeline",
        run_config=run_config
    )

    job.dagster_run_id = result["run_id"]
    job.status = "RUNNING"
    db.commit()

    return job


@router.get("/{c_id}/classifiers/jobs", response_model=List[ClassifierJobResponse])
async def list_classifier_jobs(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all classifier jobs for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    jobs = db.query(ClassifierJob).filter(
        ClassifierJob.c_id == c_id
    ).order_by(ClassifierJob.created_at.desc()).offset(skip).limit(limit).all()

    return jobs


@router.get("/{c_id}/classifiers/jobs/{job_id}", response_model=ClassifierJobResponse)
async def get_classifier_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific classifier job by ID.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(ClassifierJob).filter(
        ClassifierJob.c_id == c_id,
        ClassifierJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Classifier job with ID {job_id} not found for concept {c_id}"
        )

    return job


@router.get("/{c_id}/classifiers/jobs/{job_id}/results")
async def get_classifier_results(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get classifier training results including model scores and filtering stats.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(ClassifierJob).filter(
        ClassifierJob.c_id == c_id,
        ClassifierJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Classifier job with ID {job_id} not found for concept {c_id}"
        )

    if job.status != "COMPLETED":
        return {
            "job_id": job_id,
            "status": job.status,
            "message": f"Job is {job.status}. Results not available yet."
        }

    if not job.result_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job completed but results path not found"
        )

    # Load results JSON from S3 or local storage
    result_path = job.result_path

    if result_path.startswith("s3://"):
        import boto3
        s3_path_parts = result_path.replace("s3://", "").split("/", 1)
        s3_bucket = s3_path_parts[0]
        s3_key = s3_path_parts[1]
        local_path = f"/tmp/classifier_result_{job_id}.json"

        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        s3_client.download_file(s3_bucket, s3_key, local_path)
        with open(local_path, "r") as f:
            results = json.load(f)
    else:
        if not os.path.exists(result_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Results file not found at {result_path}"
            )
        with open(result_path, "r") as f:
            results = json.load(f)

    return {
        "job_id": job_id,
        "status": job.status,
        "filtering_stats": results.get("filtering_stats", {}),
        "model_scores": results.get("model_scores", []),
        "num_models_trained": results.get("num_models_trained", 0),
        "config_used": results.get("config_used", {}),
    }


@router.delete("/{c_id}/classifiers/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_classifier_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a classifier job.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(ClassifierJob).filter(
        ClassifierJob.c_id == c_id,
        ClassifierJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Classifier job with ID {job_id} not found for concept {c_id}"
        )

    db.delete(job)
    db.commit()

    return None


@router.post("/{c_id}/classifiers/jobs/{job_id}/cancel", response_model=ClassifierJobResponse)
async def cancel_classifier_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Cancel a running classifier job.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(ClassifierJob).filter(
        ClassifierJob.c_id == c_id,
        ClassifierJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Classifier job with ID {job_id} not found for concept {c_id}"
        )

    if job.status not in ["PENDING", "RUNNING"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status {job.status}"
        )

    job.status = "CANCELLED"
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)

    return job
