"""Entity resolution for hierarchical Snorkel pipeline.

Resolves entity IDs from any index type (SQL or derived) by walking the
dependency chain back to the root SQL index's parquet data.
"""
import os
import pandas as pd
import numpy as np
import boto3
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Create an S3 client from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _load_parquet_from_path(storage_path: str) -> pd.DataFrame:
    """Load a parquet file from S3 or local path."""
    if storage_path.startswith("s3://"):
        s3_path_parts = storage_path.replace("s3://", "").split("/", 1)
        s3_bucket = s3_path_parts[0]
        s3_key = s3_path_parts[1]
        local_path = f"/tmp/_resolve_{s3_key.replace('/', '_')}"
        s3_client = _get_s3_client()
        s3_client.download_file(s3_bucket, s3_key, local_path)
        return pd.read_parquet(local_path)
    else:
        return pd.read_parquet(storage_path)


def resolve_entity_ids(
    index_id: int,
    session: Session,
    index_column: Optional[str] = None,
) -> pd.DataFrame:
    """Resolve entity IDs from any index type.

    Returns a DataFrame from the index's underlying data source. For derived
    indexes, this walks the chain back to the root SQL index and applies
    label filters at each level.

    Args:
        index_id: The ConceptIndex ID to resolve.
        session: SQLAlchemy session for DB access.
        index_column: Optional column name to use as entity ID. Defaults to
                      the first column of the parquet.

    Returns:
        DataFrame with at minimum the entity ID column.
    """
    # Fetch the index row
    row = session.execute(
        text("""
        SELECT index_id, source_type, storage_path, parent_index_id,
               parent_snorkel_job_id, label_filter, output_type
        FROM concept_indexes
        WHERE index_id = :index_id
        """),
        {"index_id": index_id},
    ).fetchone()

    if not row:
        raise ValueError(f"Index with ID {index_id} not found")

    # --- SQL index: return its parquet data directly ---
    if row.source_type == "sql":
        if not row.storage_path:
            raise ValueError(f"SQL index {index_id} has no storage_path (not materialized?)")
        return _load_parquet_from_path(row.storage_path)

    # --- Derived index ---
    # Case 1: Root derived (parent_index_id set, no label_filter)
    if row.parent_index_id is not None and row.parent_snorkel_job_id is None:
        return resolve_entity_ids(row.parent_index_id, session, index_column)

    # Case 2: Child derived (parent_snorkel_job_id set, with optional label_filter)
    if row.parent_snorkel_job_id is not None:
        return _resolve_from_snorkel_job(
            snorkel_job_id=row.parent_snorkel_job_id,
            label_filter=row.label_filter,
            session=session,
            index_column=index_column,
            output_type=row.output_type,
        )

    raise ValueError(
        f"Derived index {index_id} has neither parent_index_id nor parent_snorkel_job_id"
    )


def _resolve_from_snorkel_job(
    snorkel_job_id: int,
    label_filter: Optional[dict],
    session: Session,
    index_column: Optional[str] = None,
    output_type: Optional[str] = None,
) -> pd.DataFrame:
    """Filter entities based on Snorkel job predictions and label filter.

    1. Load predictions parquet (sample_id + probs[]).
    2. Load cv_id_to_index from the snorkel_jobs row.
    3. For each cv_id in label_filter, find matching sample_ids.
    4. Trace back to the root index to map sample_ids → entity values.
    """
    # Load snorkel job metadata
    job_row = session.execute(
        text("""
        SELECT job_id, index_id, result_path, cv_id_to_index
        FROM snorkel_jobs
        WHERE job_id = :job_id AND status = 'COMPLETED'
        """),
        {"job_id": snorkel_job_id},
    ).fetchone()

    if not job_row:
        raise ValueError(f"Completed Snorkel job {snorkel_job_id} not found")

    # Load predictions parquet
    if not job_row.result_path:
        raise ValueError(f"Snorkel job {snorkel_job_id} has no result_path")

    df_predictions = _load_parquet_from_path(job_row.result_path)

    # Resolve the root index DataFrame (recursive)
    df_root = resolve_entity_ids(job_row.index_id, session, index_column)

    # If no label_filter, return all entities (pass-through)
    if not label_filter or "labels" not in label_filter:
        return df_root

    # Parse cv_id_to_index mapping (keys are stringified cv_ids)
    cv_id_to_index = job_row.cv_id_to_index or {}

    # Filter: for each cv_id in label_filter, find sample_ids where
    # probs[array_index] >= min_confidence
    labels_config = label_filter["labels"]
    matching_sample_ids = set()

    for cv_id_str, filter_config in labels_config.items():
        array_index = cv_id_to_index.get(str(cv_id_str))
        if array_index is None:
            logger.warning(f"cv_id {cv_id_str} not found in cv_id_to_index mapping")
            continue

        min_confidence = filter_config.get("min_confidence") if isinstance(filter_config, dict) else None

        for _, pred_row in df_predictions.iterrows():
            probs = pred_row["probs"]
            if isinstance(probs, (list, np.ndarray)) and len(probs) > array_index:
                if output_type == "argmax":
                    if np.argmax(probs) == array_index:
                        matching_sample_ids.add(int(pred_row["sample_id"]))
                else:
                    # softmax (default): threshold-based filtering
                    prob = probs[array_index]
                    if min_confidence is None or prob >= min_confidence:
                        matching_sample_ids.add(int(pred_row["sample_id"]))

    # Map sample_ids back to entity rows in the root DataFrame
    if len(matching_sample_ids) == 0:
        return df_root.iloc[0:0]  # Empty DataFrame with same schema

    matching_indices = sorted(matching_sample_ids)
    # sample_id is a positional index into the root DataFrame
    valid_indices = [i for i in matching_indices if i < len(df_root)]
    return df_root.iloc[valid_indices].reset_index(drop=True)
