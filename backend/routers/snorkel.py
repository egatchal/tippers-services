"""Snorkel training endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from backend.db.session import get_db
from backend.db.models import (
    SnorkelJob,
    ConceptIndex,
    ConceptRule,
    LabelingFunction
)
from backend.schemas import (
    SnorkelRunRequest,
    SnorkelJobResponse
)
from datetime import datetime

router = APIRouter()


@router.post("/{c_id}/snorkel/run", response_model=SnorkelJobResponse, status_code=status.HTTP_201_CREATED)
async def run_snorkel_training(
    c_id: int,
    request: SnorkelRunRequest,
    db: Session = Depends(get_db)
):
    """
    Trigger Snorkel training with configured pipeline.

    This endpoint:
    1. Validates that all assets are materialized
    2. Validates that all labeling functions are active
    3. Creates a Snorkel job record
    4. Triggers Dagster training pipeline
    5. Returns job ID for tracking

    - **c_id**: Concept ID
    - **selectedIndex**: Index ID to use as training dataset
    - **selectedRules**: List of rule IDs to include as features
    - **selectedLFs**: List of labeling function IDs to apply
    - **snorkel**: Snorkel configuration (epochs, lr, output_type)
    """
    # Validate index exists and is materialized
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == request.selectedIndex
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {request.selectedIndex} not found for concept {c_id}"
        )

    if not index.is_materialized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index '{index.name}' is not materialized. Please materialize it first."
        )

    # Validate rules exist and are materialized
    if request.selectedRules:
        rules = db.query(ConceptRule).filter(
            ConceptRule.c_id == c_id,
            ConceptRule.r_id.in_(request.selectedRules)
        ).all()

        if len(rules) != len(request.selectedRules):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or more rule IDs not found"
            )

        for rule in rules:
            if not rule.is_materialized:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Rule '{rule.name}' is not materialized. Please materialize it first."
                )

    # Validate labeling functions exist and are active
    lfs = db.query(LabelingFunction).filter(
        LabelingFunction.c_id == c_id,
        LabelingFunction.lf_id.in_(request.selectedLFs)
    ).all()

    if len(lfs) != len(request.selectedLFs):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="One or more labeling function IDs not found"
        )

    inactive_lfs = [lf.name for lf in lfs if not lf.is_active]
    if inactive_lfs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The following labeling functions are inactive: {', '.join(inactive_lfs)}"
        )

    # Validate output type
    if request.snorkel.output_type not in ["softmax", "hard_labels"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="output_type must be either 'softmax' or 'hard_labels'"
        )

    # Create Snorkel job
    job = SnorkelJob(
        c_id=c_id,
        index_id=request.selectedIndex,
        rule_ids=request.selectedRules,
        lf_ids=request.selectedLFs,
        config={
            "epochs": request.snorkel.epochs,
            "lr": request.snorkel.lr,
            "sample_size": request.snorkel.sample_size,
        },
        output_type=request.snorkel.output_type,
        status="PENDING"
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    # TODO: Trigger Dagster pipeline
    from backend.utils.dagster_client import get_dagster_client
    
    run_config = {
        "ops": {
            "snorkel_training": {
                "config": {
                    "job_id": job.job_id,
                }
            }
        }
    }
    
    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="snorkel_training_pipeline",
        run_config=run_config
    )
    
    job.dagster_run_id = result.run_id
    job.status = "RUNNING"
    db.commit()

    # # Placeholder: mark as running
    # job.dagster_run_id = f"placeholder-run-{job.job_id}"
    # job.status = "RUNNING"
    # db.commit()
    # db.refresh(job)

    return job


@router.get("/{c_id}/snorkel/jobs", response_model=List[SnorkelJobResponse])
async def list_snorkel_jobs(
    c_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all Snorkel training jobs for a concept.

    - **c_id**: Concept ID
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return
    """
    jobs = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id
    ).order_by(SnorkelJob.created_at.desc()).offset(skip).limit(limit).all()

    return jobs


@router.get("/{c_id}/snorkel/jobs/{job_id}", response_model=SnorkelJobResponse)
async def get_snorkel_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific Snorkel job by ID.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id,
        SnorkelJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Snorkel job with ID {job_id} not found for concept {c_id}"
        )

    return job


@router.get("/{c_id}/snorkel/jobs/{job_id}/results")
async def get_snorkel_results(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get Snorkel training results.

    Returns predictions in the format specified during training:
    - **softmax**: Probabilistic predictions with confidence scores
    - **hard_labels**: Binary label predictions

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id,
        SnorkelJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Snorkel job with ID {job_id} not found for concept {c_id}"
        )

    if job.status != "SUCCESS":
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

    # TODO: Load results from S3
    # from backend.utils.s3_client import load_results_from_s3
    # results = load_results_from_s3(job.result_path)
    #
    # if job.output_type == 'softmax':
    #     predictions = [
    #         {
    #             "sample_id": idx,
    #             "probs": probs.tolist(),
    #             "predicted_class": int(np.argmax(probs)),
    #             "confidence": float(np.max(probs))
    #         }
    #         for idx, probs in enumerate(results['probabilities'])
    #     ]
    #
    #     return {
    #         "job_id": job_id,
    #         "output_type": "softmax",
    #         "predictions": predictions,
    #         "summary": {
    #             "total_samples": len(predictions),
    #             "avg_confidence": float(np.mean([p['confidence'] for p in predictions]))
    #         }
    #     }
    # else:
    #     return {
    #         "job_id": job_id,
    #         "output_type": "hard_labels",
    #         "predictions": results['labels'].tolist()
    #     }

    # Placeholder response
    return {
        "job_id": job_id,
        "status": job.status,
        "output_type": job.output_type,
        "result_path": job.result_path,
        "message": "Results loading not implemented yet. Check result_path in S3."
    }


@router.delete("/{c_id}/snorkel/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_snorkel_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a Snorkel job.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id,
        SnorkelJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Snorkel job with ID {job_id} not found for concept {c_id}"
        )

    db.delete(job)
    db.commit()

    return None


@router.post("/{c_id}/snorkel/jobs/{job_id}/cancel", response_model=SnorkelJobResponse)
async def cancel_snorkel_job(
    c_id: int,
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Cancel a running Snorkel job.

    - **c_id**: Concept ID
    - **job_id**: Job ID
    """
    job = db.query(SnorkelJob).filter(
        SnorkelJob.c_id == c_id,
        SnorkelJob.job_id == job_id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Snorkel job with ID {job_id} not found for concept {c_id}"
        )

    if job.status not in ["PENDING", "RUNNING"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status {job.status}"
        )

    # TODO: Cancel Dagster run
    # from backend.utils.dagster_client import get_dagster_client
    # dagster_client = get_dagster_client()
    # dagster_client.cancel_run(job.dagster_run_id)

    job.status = "CANCELLED"
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)

    return job
