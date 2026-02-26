"""Occupancy dataset endpoints — incremental bottom-up materialization."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text as sa_text
from typing import List, Optional
from datetime import datetime
from backend.db.session import get_db
from backend.db.models import OccupancyDataset, OccupancySpaceChunk, Dataset
from backend.schemas import OccupancyDatasetCreate, OccupancyDatasetResponse
import os

router = APIRouter()

ALLOWED_INTERVALS = {900, 1800, 3600, 7200, 14400, 28800, 86400}


def _get_tippers_engine():
    url = os.getenv("TIPPERS_DB_URL")
    if not url:
        raise HTTPException(status_code=500, detail="TIPPERS_DB_URL not configured")
    return create_engine(url)


def _compute_gaps(existing_ranges, req_start, req_end):
    """
    Compute uncovered sub-ranges within [req_start, req_end) given existing
    COMPLETED (chunk_start, chunk_end) ranges sorted by chunk_start.

    Returns list of (gap_start, gap_end) tuples.
    """
    cursor = req_start
    gaps = []
    for cs, ce in existing_ranges:
        if cs > cursor:
            gaps.append((cursor, min(cs, req_end)))
        cursor = max(cursor, ce)
    if cursor < req_end:
        gaps.append((cursor, req_end))
    return gaps


@router.post("/datasets", response_model=OccupancyDatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_occupancy_dataset(
    request: OccupancyDatasetCreate,
    db: Session = Depends(get_db)
):
    """
    Create an occupancy dataset using incremental bottom-up materialization.

    One Dagster job processes the full requested time range. Existing COMPLETED
    chunks are reused — only uncovered gaps are computed. force_overwrite=true
    discards overlapping chunks and recomputes everything.
    """
    if request.interval_seconds not in ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"interval_seconds must be one of: {sorted(ALLOWED_INTERVALS)}"
        )

    # Auto-resolve missing time bounds from tippers DB
    start_time = request.start_time
    end_time = request.end_time

    tippers_engine = _get_tippers_engine()

    if start_time is None or end_time is None:
        with tippers_engine.connect() as conn:
            row = conn.execute(sa_text("""
                WITH RECURSIVE subtree AS (
                    SELECT space_id FROM space WHERE space_id = :root_space_id
                    UNION ALL
                    SELECT s.space_id FROM space s
                    JOIN subtree st ON s.parent_space_id = st.space_id
                )
                SELECT MIN(start_time) AS min_t, MAX(end_time) AS max_t
                FROM user_ap_trajectory
                WHERE space_id IN (SELECT space_id FROM subtree)
            """), {"root_space_id": request.root_space_id}).fetchone()

        if not row or row.min_t is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No session data found for space_id={request.root_space_id}"
            )
        if start_time is None:
            start_time = row.min_t
        if end_time is None:
            end_time = row.max_t

    # Strip timezone info for consistent arithmetic
    if start_time.tzinfo is not None:
        start_time = start_time.replace(tzinfo=None)
    if end_time.tzinfo is not None:
        end_time = end_time.replace(tzinfo=None)

    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_time must be after start_time"
        )

    # -------------------------------------------------------------------------
    # Query the full space subtree from tippers
    # -------------------------------------------------------------------------
    with tippers_engine.connect() as conn:
        subtree_rows = conn.execute(sa_text("""
            WITH RECURSIVE subtree AS (
                SELECT space_id, parent_space_id FROM space WHERE space_id = :root_space_id
                UNION ALL
                SELECT s.space_id, s.parent_space_id FROM space s
                JOIN subtree st ON s.parent_space_id = st.space_id
            )
            SELECT space_id, parent_space_id FROM subtree
        """), {"root_space_id": request.root_space_id}).fetchall()

    all_space_ids = [r.space_id for r in subtree_rows]
    parent_map = {r.space_id: r.parent_space_id for r in subtree_rows}
    all_space_ids_set = set(all_space_ids)

    if not all_space_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Space {request.root_space_id} not found in tippers DB"
        )

    # -------------------------------------------------------------------------
    # Find source spaces (leaves with WiFi data in the requested time range)
    # -------------------------------------------------------------------------
    with tippers_engine.connect() as conn:
        source_rows = conn.execute(sa_text("""
            SELECT DISTINCT space_id FROM user_ap_trajectory
            WHERE space_id = ANY(:space_ids)
              AND start_time < :end_time
              AND end_time   > :start_time
        """), {
            "space_ids": all_space_ids,
            "start_time": start_time,
            "end_time": end_time,
        }).fetchall()

    source_spaces = {r.space_id for r in source_rows}

    if not source_spaces:
        # Create unified Dataset parent first
        parent_ds = Dataset(
            name=request.name,
            description=request.description,
            service="occupancy",
            dataset_type="occupancy_timeseries",
            status="AVAILABLE",
            tags={"space_id": request.root_space_id},
            row_count=0,
        )
        db.add(parent_ds)
        db.flush()

        dataset = OccupancyDataset(
            name=request.name,
            description=request.description,
            root_space_id=request.root_space_id,
            start_time=start_time,
            end_time=end_time,
            interval_seconds=request.interval_seconds,
            status="COMPLETED",
            row_count=0,
            parent_dataset_id=parent_ds.dataset_id,
        )
        db.add(dataset)
        db.flush()

        parent_ds.source_ref = {"entity_type": "occupancy_dataset", "entity_id": dataset.dataset_id}
        db.commit()
        db.refresh(dataset)
        return dataset

    # -------------------------------------------------------------------------
    # Walk hierarchy upward from source spaces to find derived spaces
    # -------------------------------------------------------------------------
    derived_spaces = set()
    for space_id in source_spaces:
        current = space_id
        while True:
            parent = parent_map.get(current)
            if parent is None or parent not in all_space_ids_set:
                break
            derived_spaces.add(parent)
            current = parent

    all_relevant_spaces = source_spaces | derived_spaces
    interval = request.interval_seconds

    # -------------------------------------------------------------------------
    # Staleness detection — invalidate chunks with stale tippers data
    # -------------------------------------------------------------------------
    if not request.force_overwrite:
        for space_id in source_spaces:
            with tippers_engine.connect() as conn:
                freshness = conn.execute(sa_text("""
                    SELECT MAX(end_time) AS latest_data
                    FROM user_ap_trajectory
                    WHERE space_id = :sid
                      AND start_time < :end_time
                      AND end_time   > :start_time
                """), {
                    "sid": space_id,
                    "start_time": start_time,
                    "end_time": end_time,
                }).fetchone()

            if not freshness or freshness.latest_data is None:
                continue

            latest = freshness.latest_data
            if latest.tzinfo is not None:
                latest = latest.replace(tzinfo=None)

            # Delete COMPLETED source chunks whose range should contain this data
            # but were materialized before it arrived
            stale_deleted = db.execute(sa_text("""
                DELETE FROM occupancy_space_chunks
                WHERE space_id = :sid
                  AND interval_seconds = :interval
                  AND status = 'COMPLETED'
                  AND chunk_start < :latest
                  AND chunk_end > :start_time
                  AND completed_at < :latest
                RETURNING chunk_id
            """), {
                "sid": space_id,
                "interval": interval,
                "latest": latest,
                "start_time": start_time,
            }).fetchall()

            if stale_deleted:
                # Also invalidate derived parents that depended on this source
                current = space_id
                while True:
                    parent = parent_map.get(current)
                    if parent is None or parent not in all_space_ids_set:
                        break
                    db.execute(sa_text("""
                        DELETE FROM occupancy_space_chunks
                        WHERE space_id = :sid
                          AND interval_seconds = :interval
                          AND status = 'COMPLETED'
                          AND chunk_start < :end_time
                          AND chunk_end > :start_time
                    """), {
                        "sid": parent,
                        "interval": interval,
                        "start_time": start_time,
                        "end_time": end_time,
                    })
                    current = parent

        db.commit()

    # -------------------------------------------------------------------------
    # Incremental gap detection (or force overwrite)
    # -------------------------------------------------------------------------
    pending_count = 0

    for space_id in all_relevant_spaces:
        space_type = 'source' if space_id in source_spaces else 'derived'

        if request.force_overwrite:
            # Delete all overlapping COMPLETED/PENDING chunks for this space+interval
            db.execute(sa_text("""
                DELETE FROM occupancy_space_chunks
                WHERE space_id = :sid
                  AND interval_seconds = :interval
                  AND chunk_start < :end_time
                  AND chunk_end > :start_time
            """), {
                "sid": space_id,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            })
            gaps = [(start_time, end_time)]
        else:
            # Query existing COMPLETED chunks for this space+interval
            existing = db.execute(sa_text("""
                SELECT chunk_start, chunk_end FROM occupancy_space_chunks
                WHERE space_id = :sid
                  AND interval_seconds = :interval
                  AND status = 'COMPLETED'
                  AND chunk_start < :end_time
                  AND chunk_end > :start_time
                ORDER BY chunk_start
            """), {
                "sid": space_id,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            }).fetchall()

            existing_ranges = [(r.chunk_start, r.chunk_end) for r in existing]
            gaps = _compute_gaps(existing_ranges, start_time, end_time)

        # Insert a PENDING row for each gap
        for gap_start, gap_end in gaps:
            db.execute(sa_text("""
                INSERT INTO occupancy_space_chunks
                    (space_id, interval_seconds, chunk_start, chunk_end, space_type, status)
                VALUES
                    (:space_id, :interval, :chunk_start, :chunk_end, :space_type, 'PENDING')
                ON CONFLICT (space_id, interval_seconds, chunk_start, chunk_end) DO NOTHING
            """), {
                "space_id": space_id,
                "interval": interval,
                "chunk_start": gap_start,
                "chunk_end": gap_end,
                "space_type": space_type,
            })
            pending_count += 1

    db.commit()

    # -------------------------------------------------------------------------
    # Create OccupancyDataset record
    # -------------------------------------------------------------------------
    if pending_count == 0:
        # Everything is already materialized — mark COMPLETED immediately
        parent_ds = Dataset(
            name=request.name,
            description=request.description,
            service="occupancy",
            dataset_type="occupancy_timeseries",
            status="AVAILABLE",
            tags={"space_id": request.root_space_id},
        )
        db.add(parent_ds)
        db.flush()

        dataset = OccupancyDataset(
            name=request.name,
            description=request.description,
            root_space_id=request.root_space_id,
            start_time=start_time,
            end_time=end_time,
            interval_seconds=interval,
            status="COMPLETED",
            parent_dataset_id=parent_ds.dataset_id,
        )
        db.add(dataset)
        db.flush()

        parent_ds.source_ref = {"entity_type": "occupancy_dataset", "entity_id": dataset.dataset_id}
        db.commit()
        db.refresh(dataset)
        return dataset

    # Create unified Dataset parent first (PENDING until Dagster completes)
    parent_ds = Dataset(
        name=request.name,
        description=request.description,
        service="occupancy",
        dataset_type="occupancy_timeseries",
        status="PENDING",
        tags={"space_id": request.root_space_id},
    )
    db.add(parent_ds)
    db.flush()

    dataset = OccupancyDataset(
        name=request.name,
        description=request.description,
        root_space_id=request.root_space_id,
        start_time=start_time,
        end_time=end_time,
        interval_seconds=interval,
        status="RUNNING",
        parent_dataset_id=parent_ds.dataset_id,
    )
    db.add(dataset)
    db.flush()

    parent_ds.source_ref = {"entity_type": "occupancy_dataset", "entity_id": dataset.dataset_id}
    db.commit()
    db.refresh(dataset)

    # Create unified job tracker entry
    from backend.routers.jobs import create_unified_job
    try:
        unified_job = create_unified_job(
            db,
            service="occupancy",
            job_type="create_dataset",
            dagster_job_name="occupancy_incremental_job",
            service_job_ref={"table": "occupancy_datasets", "id": dataset.dataset_id},
            config={
                "root_space_id": request.root_space_id,
                "interval_seconds": interval,
                "force_overwrite": request.force_overwrite,
            },
            status="RUNNING",
        )
        unified_job.output_dataset_ids = [parent_ds.dataset_id]
        dataset.unified_job_id = unified_job.job_id
        db.commit()
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Submit a single Dagster job
    # -------------------------------------------------------------------------
    from backend.utils.dagster_client import get_dagster_client
    dagster_client = get_dagster_client()

    run_config = {
        "ops": {
            "plan_source_chunks": {
                "config": {"dataset_id": dataset.dataset_id}
            },
            "aggregate_and_assemble": {
                "config": {"dataset_id": dataset.dataset_id}
            },
        }
    }

    try:
        result = dagster_client.submit_job_execution(
            job_name="occupancy_incremental_job",
            run_config=run_config,
        )
        dataset.dagster_run_id = result["run_id"]
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dataset created (id={dataset.dataset_id}) but Dagster submission failed: {e}"
        )

    return dataset


@router.get("/datasets", response_model=List[OccupancyDatasetResponse])
async def list_occupancy_datasets(
    space_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List occupancy datasets.

    If space_id is given, return datasets whose subtree covers that space
    (i.e. root_space_id is the space itself or any of its ancestors).
    """
    if space_id is not None:
        # Walk up the parent chain in tippers to find all ancestors
        tippers_engine = _get_tippers_engine()
        with tippers_engine.connect() as conn:
            ancestors = conn.execute(sa_text("""
                WITH RECURSIVE ancestors AS (
                    SELECT space_id, parent_space_id FROM space WHERE space_id = :sid
                    UNION ALL
                    SELECT s.space_id, s.parent_space_id FROM space s
                    JOIN ancestors a ON s.space_id = a.parent_space_id
                )
                SELECT space_id FROM ancestors
            """), {"sid": space_id}).fetchall()
        ancestor_ids = [r.space_id for r in ancestors]

        if not ancestor_ids:
            return []

        datasets = db.query(OccupancyDataset).filter(
            OccupancyDataset.root_space_id.in_(ancestor_ids)
        ).order_by(
            OccupancyDataset.created_at.desc()
        ).offset(skip).limit(limit).all()
    else:
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
    space_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get occupancy results for a dataset.

    If space_id is provided and differs from root_space_id, return that specific
    space's chunk data from occupancy_space_chunks (for viewing child spaces).

    If COMPLETED: download the dataset parquet and return rows.
    If RUNNING: return progress (COMPLETED vs total chunks).
    If FAILED: return error message.
    """
    import pandas as pd
    import boto3

    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    # Use requested space_id or fall back to dataset root
    view_space_id = space_id if space_id is not None else dataset.root_space_id

    if dataset.status == 'FAILED':
        return {
            "dataset_id": dataset_id,
            "space_id": view_space_id,
            "status": "FAILED",
            "error": dataset.error_message,
            "completed_chunks": 0,
            "total_chunks": 0,
        }

    if dataset.status == 'RUNNING':
        # Count chunks for progress (all chunks for this interval overlapping the range)
        chunk_counts = db.execute(sa_text("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed
            FROM occupancy_space_chunks
            WHERE interval_seconds = :interval
              AND chunk_start < :end_time
              AND chunk_end > :start_time
        """), {
            "interval": dataset.interval_seconds,
            "start_time": dataset.start_time,
            "end_time": dataset.end_time,
        }).fetchone()

        return {
            "dataset_id": dataset_id,
            "space_id": view_space_id,
            "status": "RUNNING",
            "completed_chunks": chunk_counts.completed if chunk_counts else 0,
            "total_chunks": chunk_counts.total if chunk_counts else 0,
        }

    # COMPLETED — read parquet data
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    # If viewing a specific child space, load its chunks directly
    if view_space_id != dataset.root_space_id:
        chunks = db.execute(sa_text("""
            SELECT storage_path, chunk_start FROM occupancy_space_chunks
            WHERE space_id = :sid
              AND interval_seconds = :interval
              AND status = 'COMPLETED'
              AND chunk_start < :end_time
              AND chunk_end > :start_time
            ORDER BY chunk_start
        """), {
            "sid": view_space_id,
            "interval": dataset.interval_seconds,
            "start_time": dataset.start_time,
            "end_time": dataset.end_time,
        }).fetchall()

        dfs = []
        for chunk in chunks:
            if not chunk.storage_path:
                continue
            sp = chunk.storage_path
            if sp.startswith("s3://"):
                parts = sp.replace("s3://", "").split("/", 1)
                lp = f"/tmp/space_{view_space_id}_chunk_{len(dfs)}.parquet"
                s3_client.download_file(parts[0], parts[1], lp)
                dfs.append(pd.read_parquet(lp))
            elif os.path.exists(sp):
                dfs.append(pd.read_parquet(sp))

        if dfs:
            df = pd.concat(dfs, ignore_index=True).sort_values('interval_begin_time').reset_index(drop=True)
        else:
            df = pd.DataFrame(columns=['interval_begin_time', 'number_connections'])
    else:
        # Root space — use the assembled dataset parquet
        if not dataset.storage_path:
            return {
                "dataset_id": dataset_id,
                "space_id": view_space_id,
                "status": "COMPLETED",
                "completed_chunks": 0,
                "total_chunks": 0,
                "row_count": dataset.row_count or 0,
                "rows": [],
            }

        sp = dataset.storage_path
        if sp.startswith("s3://"):
            parts = sp.replace("s3://", "").split("/", 1)
            local_path = f"/tmp/results_{dataset_id}.parquet"
            s3_client.download_file(parts[0], parts[1], local_path)
            df = pd.read_parquet(local_path)
        elif os.path.exists(sp):
            df = pd.read_parquet(sp)
        else:
            df = pd.DataFrame(columns=['interval_begin_time', 'number_connections'])

    rows = df.head(500).to_dict(orient='records')

    return {
        "dataset_id": dataset_id,
        "space_id": view_space_id,
        "status": "COMPLETED",
        "completed_chunks": 0,
        "total_chunks": 0,
        "row_count": len(rows),
        "rows": rows,
    }


@router.post("/datasets/{dataset_id}/retry")
async def retry_occupancy_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Reset FAILED chunks for this dataset's spaces and resubmit a single Dagster job.
    """
    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    # Reset all FAILED chunks within this dataset's time range + interval
    reset_result = db.execute(sa_text("""
        UPDATE occupancy_space_chunks
        SET status = 'PENDING',
            dagster_run_id = NULL,
            error_message = NULL,
            completed_at = NULL
        WHERE interval_seconds = :interval
          AND status = 'FAILED'
          AND chunk_start < :end_time
          AND chunk_end > :start_time
        RETURNING chunk_id
    """), {
        "interval": dataset.interval_seconds,
        "start_time": dataset.start_time,
        "end_time": dataset.end_time,
    }).fetchall()

    db.commit()

    reset_count = len(reset_result)
    if reset_count == 0:
        return {"reset": 0, "resubmitted": False}

    # Mark dataset RUNNING again
    dataset.status = 'RUNNING'
    dataset.error_message = None
    db.commit()

    # Submit a single incremental job
    from backend.utils.dagster_client import get_dagster_client
    dagster_client = get_dagster_client()

    run_config = {
        "ops": {
            "plan_source_chunks": {
                "config": {"dataset_id": dataset.dataset_id}
            },
            "aggregate_and_assemble": {
                "config": {"dataset_id": dataset.dataset_id}
            },
        }
    }

    try:
        result = dagster_client.submit_job_execution(
            job_name="occupancy_incremental_job",
            run_config=run_config,
        )
        dataset.dagster_run_id = result["run_id"]
        db.commit()
    except Exception as e:
        return {"reset": reset_count, "resubmitted": False, "error": str(e)}

    return {"reset": reset_count, "resubmitted": True}


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


@router.delete("/cleanup")
async def cleanup_stale_occupancy_data(
    max_age_hours: int = 24,
    db: Session = Depends(get_db)
):
    """
    Purge stale PENDING/FAILED/RUNNING occupancy records older than max_age_hours.

    Deletes stale chunks and marks stale jobs/datasets as FAILED.
    """
    # Delete stale chunks
    deleted_chunks = db.execute(sa_text("""
        DELETE FROM occupancy_space_chunks
        WHERE status IN ('PENDING', 'FAILED', 'RUNNING')
          AND created_at < NOW() - make_interval(hours => :hours)
        RETURNING chunk_id
    """), {"hours": max_age_hours}).fetchall()

    # Mark stale jobs as FAILED
    failed_jobs = db.execute(sa_text("""
        UPDATE jobs
        SET status = 'FAILED', error_message = 'Manual cleanup: stale record'
        WHERE status IN ('PENDING', 'RUNNING')
          AND service = 'occupancy'
          AND created_at < NOW() - make_interval(hours => :hours)
        RETURNING job_id
    """), {"hours": max_age_hours}).fetchall()

    # Mark stale occupancy_datasets as FAILED
    failed_datasets = db.execute(sa_text("""
        UPDATE occupancy_datasets
        SET status = 'FAILED', error_message = 'Manual cleanup: stale record'
        WHERE status IN ('PENDING', 'RUNNING')
          AND created_at < NOW() - make_interval(hours => :hours)
        RETURNING dataset_id
    """), {"hours": max_age_hours}).fetchall()

    db.commit()

    return {
        "deleted_chunks": len(deleted_chunks),
        "failed_jobs": len(failed_jobs),
        "failed_datasets": len(failed_datasets),
    }
