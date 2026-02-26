"""Unified job tracker endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from backend.db.session import get_db
from backend.db.models import Job
from backend.schemas import JobResponse

router = APIRouter()


@router.get("", response_model=List[JobResponse])
async def list_jobs(
    service: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    job_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List jobs with optional filters."""
    query = db.query(Job)
    if service:
        query = query.filter(Job.service == service)
    if status_filter:
        query = query.filter(Job.status == status_filter)
    if job_type:
        query = query.filter(Job.job_type == job_type)
    return query.order_by(Job.created_at.desc()).offset(skip).limit(limit).all()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: int, db: Session = Depends(get_db)):
    """Get a specific job by ID."""
    job = db.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@router.get("/{job_id}/logs")
async def get_job_logs(job_id: int, db: Session = Depends(get_db)):
    """Proxy to Dagster logs for this job's run."""
    job = db.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if not job.dagster_run_id:
        return {"job_id": job_id, "logs": [], "message": "No Dagster run associated"}

    from backend.utils.dagster_client import get_dagster_client
    try:
        dagster_client = get_dagster_client()
        run_status = dagster_client.get_run_status(job.dagster_run_id)
        return {
            "job_id": job_id,
            "dagster_run_id": job.dagster_run_id,
            "dagster_status": run_status.get("status"),
            "start_time": run_status.get("start_time"),
            "end_time": run_status.get("end_time"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch logs: {e}")


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(job_id: int, db: Session = Depends(get_db)):
    """Cancel a running job via Dagster."""
    job = db.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if job.status not in ("PENDING", "RUNNING"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status {job.status}",
        )

    job.status = "CANCELLED"
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return job


def create_unified_job(
    db: Session,
    *,
    service: str,
    job_type: str,
    dagster_run_id: str = None,
    dagster_job_name: str = None,
    service_job_ref: dict = None,
    config: dict = None,
    status: str = "PENDING",
) -> Job:
    """Helper to create a unified Job row. Called from service-specific routers."""
    job = Job(
        service=service,
        job_type=job_type,
        dagster_run_id=dagster_run_id,
        dagster_job_name=dagster_job_name,
        service_job_ref=service_job_ref,
        config=config,
        status=status,
        started_at=datetime.utcnow() if status == "RUNNING" else None,
    )
    db.add(job)
    db.flush()  # Get job_id without committing
    return job
