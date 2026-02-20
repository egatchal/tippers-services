"""Occupancy dataset endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text as sa_text
from typing import List, Optional
from datetime import datetime, timedelta
from backend.db.session import get_db
from backend.db.models import OccupancyDataset, OccupancySpaceChunk
from backend.schemas import OccupancyDatasetCreate, OccupancyDatasetResponse
import os

router = APIRouter()

ALLOWED_INTERVALS = {900, 1800, 3600, 7200, 14400, 28800, 86400}


def _get_tippers_engine():
    url = os.getenv("TIPPERS_DB_URL")
    if not url:
        raise HTTPException(status_code=500, detail="TIPPERS_DB_URL not configured")
    return create_engine(url)


def _compute_aligned_chunks(
    start_time: datetime,
    end_time: datetime,
    chunk_days: int,
) -> list:
    """Return epoch-aligned (chunk_start, chunk_end) tuples covering [start_time, end_time)."""
    chunk_secs = chunk_days * 86400
    # Strip timezone info for arithmetic
    if start_time.tzinfo is not None:
        start_time = start_time.replace(tzinfo=None)
    if end_time.tzinfo is not None:
        end_time = end_time.replace(tzinfo=None)
    epoch = datetime(1970, 1, 1)
    start_ts = (start_time - epoch).total_seconds()
    aligned_ts = (start_ts // chunk_secs) * chunk_secs
    t = epoch + timedelta(seconds=aligned_ts)
    chunks = []
    while t < end_time:
        chunks.append((t, t + timedelta(seconds=chunk_secs)))
        t += timedelta(seconds=chunk_secs)
    return chunks


@router.post("/datasets", response_model=OccupancyDatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_occupancy_dataset(
    request: OccupancyDatasetCreate,
    db: Session = Depends(get_db)
):
    """
    Create an occupancy dataset using per-space chunk materialization.

    For each space in the subtree (bottom-up) and each epoch-aligned chunk window,
    an OccupancySpaceChunk record is created. Source chunks (rooms with WiFi data)
    are submitted immediately as Dagster jobs. Derived chunks (floors, buildings)
    are triggered by a sensor once their children complete. Chunks are reused across
    datasets — a COMPLETED chunk is never re-run.
    """
    if request.interval_seconds not in ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"interval_seconds must be one of: {sorted(ALLOWED_INTERVALS)}"
        )

    chunk_days = request.chunk_days or 30  # default 30-day chunks if not specified

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

    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_time must be after start_time"
        )

    # -------------------------------------------------------------------------
    # Query the full space subtree + parent-child relationships from tippers
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
    # Find source spaces (those with session data in the requested time range)
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
        # No data in range — create dataset record and return immediately as COMPLETED
        dataset = OccupancyDataset(
            name=request.name,
            description=request.description,
            root_space_id=request.root_space_id,
            start_time=start_time,
            end_time=end_time,
            interval_seconds=request.interval_seconds,
            chunk_days=chunk_days,
            status="COMPLETED",
            row_count=0,
        )
        db.add(dataset)
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

    # -------------------------------------------------------------------------
    # Compute epoch-aligned chunk windows
    # -------------------------------------------------------------------------
    chunks = _compute_aligned_chunks(start_time, end_time, chunk_days)

    # -------------------------------------------------------------------------
    # Upsert OccupancySpaceChunk records for all (space, chunk) combinations.
    # ON CONFLICT DO NOTHING skips COMPLETED chunks, preserving existing records.
    # -------------------------------------------------------------------------
    for space_id in source_spaces:
        for chunk_start, chunk_end in chunks:
            db.execute(sa_text("""
                INSERT INTO occupancy_space_chunks
                    (space_id, interval_seconds, chunk_start, chunk_end, space_type, status)
                VALUES
                    (:space_id, :interval, :chunk_start, :chunk_end, 'source', 'PENDING')
                ON CONFLICT (space_id, interval_seconds, chunk_start, chunk_end) DO NOTHING
            """), {
                "space_id": space_id,
                "interval": request.interval_seconds,
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
            })

    for space_id in derived_spaces:
        for chunk_start, chunk_end in chunks:
            db.execute(sa_text("""
                INSERT INTO occupancy_space_chunks
                    (space_id, interval_seconds, chunk_start, chunk_end, space_type, status)
                VALUES
                    (:space_id, :interval, :chunk_start, :chunk_end, 'derived', 'PENDING')
                ON CONFLICT (space_id, interval_seconds, chunk_start, chunk_end) DO NOTHING
            """), {
                "space_id": space_id,
                "interval": request.interval_seconds,
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
            })

    db.commit()

    # -------------------------------------------------------------------------
    # Create OccupancyDataset record first so it always exists
    # -------------------------------------------------------------------------
    dataset = OccupancyDataset(
        name=request.name,
        description=request.description,
        root_space_id=request.root_space_id,
        start_time=start_time,
        end_time=end_time,
        interval_seconds=request.interval_seconds,
        chunk_days=chunk_days,
        status="RUNNING",
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    # -------------------------------------------------------------------------
    # Submit Dagster jobs for PENDING source chunks that have no run yet
    # -------------------------------------------------------------------------
    pending_source = db.execute(sa_text("""
        SELECT chunk_id FROM occupancy_space_chunks
        WHERE space_type = 'source'
          AND status = 'PENDING'
          AND dagster_run_id IS NULL
          AND space_id = ANY(:space_ids)
          AND interval_seconds = :interval
    """), {
        "space_ids": list(source_spaces),
        "interval": request.interval_seconds,
    }).fetchall()

    from backend.utils.dagster_client import get_dagster_client
    dagster_client = get_dagster_client()

    submitted_count = 0
    submission_errors = []
    for row in pending_source:
        chunk_id = row.chunk_id
        run_config = {
            "ops": {
                "materialize_source_chunk": {
                    "config": {"chunk_id": chunk_id}
                }
            }
        }
        try:
            result = dagster_client.submit_job_execution(
                job_name="materialize_source_chunk_job",
                run_config=run_config,
            )
            db.execute(sa_text("""
                UPDATE occupancy_space_chunks
                SET dagster_run_id = :run_id
                WHERE chunk_id = :chunk_id
            """), {"run_id": result["run_id"], "chunk_id": chunk_id})
            submitted_count += 1
        except Exception as e:
            submission_errors.append(str(e))

    db.commit()

    if submission_errors:
        # Surface first error — dataset record still exists, chunks stay PENDING
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dataset created (id={dataset.dataset_id}) but Dagster submission failed: {submission_errors[0]}"
        )

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
    space_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get occupancy results for a specific space within a dataset.

    space_id defaults to the dataset's root_space_id.

    Returns per-chunk progress while computing, or concatenated parquet rows
    once all chunks for the requested space are COMPLETED.
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

    target_space_id = space_id if space_id is not None else dataset.root_space_id

    # Compute expected chunk windows for this dataset
    effective_chunk_days = dataset.chunk_days or 30
    chunks = _compute_aligned_chunks(dataset.start_time, dataset.end_time, effective_chunk_days)
    total_chunks = len(chunks)

    # Query OccupancySpaceChunk records for this space + interval
    chunk_records = db.query(OccupancySpaceChunk).filter(
        OccupancySpaceChunk.space_id == target_space_id,
        OccupancySpaceChunk.interval_seconds == dataset.interval_seconds,
    ).all()

    # Filter to the expected windows
    expected_set = {(cs, ce) for cs, ce in chunks}
    relevant = [r for r in chunk_records if (r.chunk_start, r.chunk_end) in expected_set]

    if not relevant:
        return {
            "dataset_id": dataset_id,
            "space_id": target_space_id,
            "status": "PENDING",
            "message": "No chunk records found for this space. Data may not exist or jobs are queued.",
            "completed_chunks": 0,
            "total_chunks": total_chunks,
        }

    completed = [r for r in relevant if r.status == 'COMPLETED']
    failed = [r for r in relevant if r.status == 'FAILED']
    in_progress = [r for r in relevant if r.status in ('PENDING', 'RUNNING')]

    if failed:
        return {
            "dataset_id": dataset_id,
            "space_id": target_space_id,
            "status": "FAILED",
            "error": failed[0].error_message,
            "completed_chunks": len(completed),
            "total_chunks": total_chunks,
        }

    if in_progress:
        return {
            "dataset_id": dataset_id,
            "space_id": target_space_id,
            "status": "RUNNING",
            "completed_chunks": len(completed),
            "total_chunks": total_chunks,
        }

    # All COMPLETED — load and concatenate parquets
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    dfs = []
    for record in sorted(relevant, key=lambda r: r.chunk_start):
        if not record.storage_path:
            continue
        if record.storage_path.startswith("s3://"):
            parts = record.storage_path.replace("s3://", "").split("/", 1)
            local_path = f"/tmp/results_{dataset_id}_space_{target_space_id}_chunk_{record.chunk_id}.parquet"
            s3_client.download_file(parts[0], parts[1], local_path)
            dfs.append(pd.read_parquet(local_path))
        elif os.path.exists(record.storage_path):
            dfs.append(pd.read_parquet(record.storage_path))

    if dfs:
        df = pd.concat(dfs, ignore_index=True).sort_values('interval_begin_time')
        rows = df.head(500).to_dict(orient='records')
    else:
        rows = []

    # Lazily mark dataset COMPLETED if the root space is fully done
    if (
        dataset.status == 'RUNNING'
        and target_space_id == dataset.root_space_id
        and len(completed) == total_chunks
    ):
        dataset.status = 'COMPLETED'
        db.commit()

    return {
        "dataset_id": dataset_id,
        "space_id": target_space_id,
        "status": "COMPLETED",
        "completed_chunks": len(completed),
        "total_chunks": total_chunks,
        "row_count": len(rows),
        "rows": rows,
    }


@router.post("/datasets/{dataset_id}/retry")
async def retry_occupancy_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Reset FAILED chunks for this dataset and resubmit their Dagster jobs.

    - FAILED source chunks → reset to PENDING, resubmit immediately
    - FAILED derived chunks → reset to PENDING, picked up by sensor once sources complete
    """
    dataset = db.query(OccupancyDataset).filter(
        OccupancyDataset.dataset_id == dataset_id
    ).first()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Occupancy dataset with ID {dataset_id} not found"
        )

    # Compute the expected chunk windows for this dataset
    effective_chunk_days = dataset.chunk_days or 30
    chunks = _compute_aligned_chunks(dataset.start_time, dataset.end_time, effective_chunk_days)
    chunk_windows = [(cs, ce) for cs, ce in chunks]

    if not chunk_windows:
        return {"reset": 0, "resubmitted": 0}

    # Reset all FAILED chunks within this dataset's time range + interval
    min_chunk_start = chunk_windows[0][0]
    max_chunk_end = chunk_windows[-1][1]

    reset_result = db.execute(sa_text("""
        UPDATE occupancy_space_chunks
        SET status = 'PENDING',
            dagster_run_id = NULL,
            error_message = NULL,
            completed_at = NULL
        WHERE interval_seconds = :interval
          AND status = 'FAILED'
          AND chunk_start >= :min_start
          AND chunk_end <= :max_end
        RETURNING chunk_id, space_type
    """), {
        "interval": dataset.interval_seconds,
        "min_start": min_chunk_start,
        "max_end": max_chunk_end,
    }).fetchall()

    db.commit()

    reset_count = len(reset_result)
    source_chunk_ids = [r.chunk_id for r in reset_result if r.space_type == 'source']

    if not source_chunk_ids:
        # Only derived chunks were reset — sensor will pick them up once sources finish
        return {"reset": reset_count, "resubmitted": 0}

    # Resubmit source chunk jobs
    from backend.utils.dagster_client import get_dagster_client
    dagster_client = get_dagster_client()

    resubmitted = 0
    errors = []
    for chunk_id in source_chunk_ids:
        run_config = {
            "ops": {
                "materialize_source_chunk": {
                    "config": {"chunk_id": chunk_id}
                }
            }
        }
        try:
            result = dagster_client.submit_job_execution(
                job_name="materialize_source_chunk_job",
                run_config=run_config,
            )
            db.execute(sa_text("""
                UPDATE occupancy_space_chunks
                SET dagster_run_id = :run_id
                WHERE chunk_id = :chunk_id
            """), {"run_id": result["run_id"], "chunk_id": chunk_id})
            resubmitted += 1
        except Exception as e:
            errors.append(str(e))

    db.commit()

    # Mark dataset RUNNING again if it was somehow marked COMPLETED/FAILED
    if dataset.status not in ('RUNNING',):
        dataset.status = 'RUNNING'
        db.commit()

    response = {"reset": reset_count, "resubmitted": resubmitted}
    if errors:
        response["errors"] = errors
    return response


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
