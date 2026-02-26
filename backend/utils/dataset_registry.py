"""Unified dataset catalog helper — register data artifacts from Dagster assets."""
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def register_dataset(
    session: Session,
    *,
    name: str,
    service: str,
    dataset_type: str,
    storage_path: str,
    row_count: Optional[int] = None,
    column_stats: Optional[dict] = None,
    schema_info: Optional[dict] = None,
    source_ref: Optional[dict] = None,
    tags: Optional[dict] = None,
    format: str = "parquet",
    description: Optional[str] = None,
) -> int:
    """Insert or update a row in the unified `datasets` table.

    If a row with the same (service, dataset_type, source_ref) already exists,
    it is updated rather than duplicated.  Returns the dataset_id.
    """
    # Check for existing entry with same source_ref
    existing_id = None
    if source_ref:
        result = session.execute(
            text("""
                SELECT dataset_id FROM datasets
                WHERE service = :service
                  AND dataset_type = :dataset_type
                  AND source_ref @> CAST(:source_ref AS jsonb)
                LIMIT 1
            """),
            {
                "service": service,
                "dataset_type": dataset_type,
                "source_ref": json.dumps(source_ref),
            },
        )
        row = result.fetchone()
        if row:
            existing_id = row.dataset_id

    now = datetime.utcnow()

    if existing_id:
        session.execute(
            text("""
                UPDATE datasets
                SET name = :name,
                    description = :description,
                    storage_path = :storage_path,
                    row_count = :row_count,
                    column_stats = CAST(:column_stats AS jsonb),
                    schema_info = CAST(:schema_info AS jsonb),
                    tags = CAST(:tags AS jsonb),
                    format = :format,
                    status = 'AVAILABLE',
                    updated_at = :now
                WHERE dataset_id = :dataset_id
            """),
            {
                "name": name,
                "description": description,
                "storage_path": storage_path,
                "row_count": row_count,
                "column_stats": json.dumps(column_stats) if column_stats else None,
                "schema_info": json.dumps(schema_info) if schema_info else None,
                "tags": json.dumps(tags) if tags else None,
                "format": format,
                "now": now,
                "dataset_id": existing_id,
            },
        )
        session.commit()
        logger.info(f"Updated dataset catalog entry {existing_id} for {service}/{dataset_type}")
        return existing_id
    else:
        result = session.execute(
            text("""
                INSERT INTO datasets
                    (name, description, service, dataset_type, format, storage_path,
                     source_ref, row_count, column_stats, schema_info, tags,
                     status, created_at, updated_at)
                VALUES
                    (:name, :description, :service, :dataset_type, :format, :storage_path,
                     CAST(:source_ref AS jsonb), :row_count,
                     CAST(:column_stats AS jsonb), CAST(:schema_info AS jsonb),
                     CAST(:tags AS jsonb),
                     'AVAILABLE', :now, :now)
                RETURNING dataset_id
            """),
            {
                "name": name,
                "description": description,
                "service": service,
                "dataset_type": dataset_type,
                "format": format,
                "storage_path": storage_path,
                "source_ref": json.dumps(source_ref) if source_ref else None,
                "row_count": row_count,
                "column_stats": json.dumps(column_stats) if column_stats else None,
                "schema_info": json.dumps(schema_info) if schema_info else None,
                "tags": json.dumps(tags) if tags else None,
                "now": now,
            },
        )
        dataset_id = result.fetchone().dataset_id
        session.commit()
        logger.info(f"Registered new dataset catalog entry {dataset_id} for {service}/{dataset_type}")
        return dataset_id
