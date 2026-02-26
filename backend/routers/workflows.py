"""Workflow template CRUD and execution endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from backend.db.session import get_db
from backend.db.models import WorkflowTemplate, WorkflowRun
from backend.schemas import (
    WorkflowTemplateCreate,
    WorkflowTemplateResponse,
    WorkflowRunRequest,
    WorkflowRunResponse,
)
from backend.utils.workflow_engine import WorkflowEngine

router = APIRouter()


# ── Templates ────────────────────────────────────────────────────────────────

@router.post("/templates", response_model=WorkflowTemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(request: WorkflowTemplateCreate, db: Session = Depends(get_db)):
    """Create a workflow template."""
    tmpl = WorkflowTemplate(
        name=request.name,
        service=request.service,
        description=request.description,
        steps=request.steps,
    )
    db.add(tmpl)
    db.commit()
    db.refresh(tmpl)
    return tmpl


@router.get("/templates", response_model=List[WorkflowTemplateResponse])
async def list_templates(
    service: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List workflow templates, optionally filtered by service."""
    query = db.query(WorkflowTemplate).filter(WorkflowTemplate.is_active == True)
    if service:
        query = query.filter(WorkflowTemplate.service == service)
    return query.order_by(WorkflowTemplate.created_at.desc()).offset(skip).limit(limit).all()


@router.get("/templates/{template_id}", response_model=WorkflowTemplateResponse)
async def get_template(template_id: int, db: Session = Depends(get_db)):
    """Get a workflow template."""
    tmpl = db.query(WorkflowTemplate).filter(WorkflowTemplate.template_id == template_id).first()
    if not tmpl:
        raise HTTPException(status_code=404, detail=f"Template {template_id} not found")
    return tmpl


# ── Runs ─────────────────────────────────────────────────────────────────────

@router.get("/runs", response_model=List[WorkflowRunResponse])
async def list_runs(
    service: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List workflow runs, optionally filtered by service or status."""
    query = db.query(WorkflowRun)
    if service:
        query = query.filter(WorkflowRun.service == service)
    if status_filter:
        query = query.filter(WorkflowRun.status == status_filter.upper())
    return query.order_by(WorkflowRun.created_at.desc()).offset(skip).limit(limit).all()


@router.post("/run", response_model=WorkflowRunResponse, status_code=status.HTTP_201_CREATED)
async def execute_workflow(request: WorkflowRunRequest, db: Session = Depends(get_db)):
    """Execute a workflow template with runtime params."""
    engine = WorkflowEngine(db)
    try:
        result = engine.start_run(request.template_id, request.params)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Workflow execution failed: {e}")

    run = db.query(WorkflowRun).filter(WorkflowRun.run_id == result["run_id"]).first()
    return run


@router.get("/runs/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run(run_id: int, db: Session = Depends(get_db)):
    """Get workflow run status with per-step breakdown."""
    run = db.query(WorkflowRun).filter(WorkflowRun.run_id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Workflow run {run_id} not found")
    return run


@router.post("/runs/{run_id}/cancel", response_model=WorkflowRunResponse)
async def cancel_workflow_run(run_id: int, db: Session = Depends(get_db)):
    """Cancel all running steps in a workflow run."""
    run = db.query(WorkflowRun).filter(WorkflowRun.run_id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Workflow run {run_id} not found")

    if run.status not in ("PENDING", "RUNNING"):
        raise HTTPException(status_code=400, detail=f"Cannot cancel run with status {run.status}")

    run.status = "CANCELLED"
    run.completed_at = datetime.utcnow()

    # Mark all non-completed steps as CANCELLED
    if run.step_statuses:
        import json
        statuses = run.step_statuses if isinstance(run.step_statuses, dict) else json.loads(run.step_statuses)
        for key, ss in statuses.items():
            if ss.get("status") in ("PENDING", "RUNNING"):
                ss["status"] = "CANCELLED"
        run.step_statuses = statuses

    db.commit()
    db.refresh(run)
    return run
