"""Dagster assets for weak supervision pipeline."""
from dagster import (
    asset, AssetExecutionContext, Output,
    op, OpExecutionContext,
    DynamicOutput, DynamicOut,
    graph,
    run_failure_sensor, RunFailureSensorContext,
    sensor, SensorEvaluationContext, RunRequest,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any
import json
from cryptography.fernet import Fernet
from jinja2 import Template


def compute_column_stats(df: pd.DataFrame) -> dict:
    """Compute per-column statistics for caching alongside materialized data."""
    stats = {}
    for col in df.columns:
        s = {"dtype": str(df[col].dtype), "null_count": int(df[col].isna().sum())}
        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            s.update({
                "min": float(desc["min"]),
                "max": float(desc["max"]),
                "mean": round(float(desc["mean"]), 4),
                "std": round(float(desc["std"]), 4),
            })
        else:
            s.update({
                "unique_count": int(df[col].nunique()),
                "top_values": {str(k): int(v) for k, v in df[col].value_counts().head(5).items()},
            })
        stats[col] = s
    return stats


def get_db_engine():
    """Get database engine from environment."""
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/tippers")
    return create_engine(database_url)


def get_tippers_engine():
    """Get engine for the external tippers source DB (user_ap_trajectory, space tables)."""
    tippers_db_url = os.getenv("TIPPERS_DB_URL")
    if not tippers_db_url:
        raise ValueError("TIPPERS_DB_URL environment variable is not set")
    return create_engine(tippers_db_url)


def get_storage_path(asset_type: str, asset_id: int) -> str:
    """Get storage path for an asset."""
    base_path = os.getenv("STORAGE_PATH", "/tmp/tippers-data")
    os.makedirs(base_path, exist_ok=True)
    return f"{base_path}/{asset_type}_{asset_id}.parquet"


def decrypt_password(encrypted_password: str) -> str:
    """Decrypt password using encryption key from environment."""
    encryption_key = os.getenv("ENCRYPTION_KEY")
    if not encryption_key:
        raise ValueError("ENCRYPTION_KEY environment variable not set")
    cipher_suite = Fernet(encryption_key.encode() if isinstance(encryption_key, str) else encryption_key)
    return cipher_suite.decrypt(encrypted_password.encode()).decode()


OCCUPANCY_SQL = """
WITH RECURSIVE subtree AS (
    SELECT space_id FROM space WHERE space_id = :root_space_id
    UNION ALL
    SELECT s.space_id FROM space s
    JOIN subtree st ON s.parent_space_id = st.space_id
),
closure AS (
    SELECT space_id AS descendant_id, space_id AS ancestor_id
    FROM subtree
    UNION ALL
    SELECT c.descendant_id, sp.parent_space_id AS ancestor_id
    FROM closure c
    JOIN space sp ON sp.space_id = c.ancestor_id
    JOIN subtree st ON st.space_id = sp.parent_space_id
    WHERE sp.parent_space_id IS NOT NULL
),
bins AS (
    SELECT DISTINCT
        to_timestamp(gs)::timestamp AS bin_start
    FROM user_ap_trajectory sess
    JOIN subtree st ON sess.space_id = st.space_id
    CROSS JOIN LATERAL generate_series(
        floor(extract(epoch from GREATEST(sess.start_time, CAST(:chunk_start AS timestamp))) / :interval_seconds)::bigint * :interval_seconds,
        floor((extract(epoch from LEAST(sess.end_time, CAST(:chunk_end AS timestamp)) - '1 microsecond'::interval)) / :interval_seconds)::bigint * :interval_seconds,
        :interval_seconds
    ) gs
    WHERE sess.start_time < CAST(:chunk_end AS timestamp)
      AND sess.end_time > CAST(:chunk_start AS timestamp)
),
leaf_counts AS (
    SELECT
        st.space_id,
        b.bin_start,
        COUNT(DISTINCT sess.mac_address) AS connections
    FROM subtree st
    CROSS JOIN bins b
    LEFT JOIN user_ap_trajectory sess ON
        sess.space_id = st.space_id
        AND sess.start_time < b.bin_start + (:interval_seconds || ' seconds')::interval
        AND sess.end_time > b.bin_start
    GROUP BY st.space_id, b.bin_start
)
SELECT
    c.ancestor_id AS space_id,
    lc.bin_start AS interval_begin_time,
    SUM(lc.connections) AS number_connections
FROM closure c
JOIN leaf_counts lc ON lc.space_id = c.descendant_id
GROUP BY c.ancestor_id, lc.bin_start
ORDER BY c.ancestor_id, lc.bin_start
"""


@asset(
    group_name="data_materialization",
    description="Materializes index data from external database",
    required_resource_keys={"s3_storage"},
    config_schema={"index_id": int}
)
def materialized_index(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Materialize concept index data from external database.

    Reads the index definition from the database, connects to the external
    database, executes the SQL query, and stores the results.

    Returns metadata about the materialized data.
    """
    # Get index_id from config
    index_id = context.op_config.get("index_id")

    if not index_id:
        raise ValueError("index_id must be provided in op_config")

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get index metadata
        context.log.info(f"Loading index metadata for index_id={index_id}")

        result = session.execute(
            text("""
            SELECT ci.*, dc.host, dc.port, dc.database, dc.user, dc.encrypted_password, dc.connection_type
            FROM concept_indexes ci
            JOIN database_connections dc ON ci.conn_id = dc.conn_id
            WHERE ci.index_id = :index_id
            """),
            {"index_id": index_id}
        )

        index_row = result.fetchone()

        if not index_row:
            raise ValueError(f"Index with ID {index_id} not found")

        # Build connection string to external database
        conn_type = index_row.connection_type
        user = index_row.user
        password = decrypt_password(index_row.encrypted_password)
        host = index_row.host
        port = index_row.port
        database = index_row.database

        external_conn_str = f"{conn_type}://{user}:{password}@{host}:{port}/{database}"

        context.log.info(f"Connecting to external database: {host}:{port}/{database}")

        # Execute query on external database
        external_engine = create_engine(external_conn_str)
        sql_query = index_row.sql_query

        context.log.info(f"Executing query: {sql_query[:100]}...")

        df = pd.read_sql(sql_query, external_engine)

        context.log.info(f"Query returned {len(df)} rows")

        # Compute column stats
        column_stats = compute_column_stats(df)

        # Store results locally
        local_storage_path = get_storage_path("index", index_id)
        df.to_parquet(local_storage_path, index=False)

        context.log.info(f"Data saved locally to {local_storage_path}")

        # Upload to S3
        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"indexes/index_{index_id}.parquet"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Data uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # Update index metadata
        session.execute(
            text("""
            UPDATE concept_indexes
            SET is_materialized = true,
                materialized_at = :now,
                row_count = :row_count,
                column_stats = :column_stats,
                storage_path = :storage_path,
                updated_at = :now
            WHERE index_id = :index_id
            """),
            {
                "index_id": index_id,
                "now": datetime.utcnow(),
                "row_count": len(df),
                "column_stats": json.dumps(column_stats),
                "storage_path": storage_path
            }
        )

        session.commit()

        context.log.info(f"Index {index_id} materialization complete")

        return Output(
            value={
                "index_id": index_id,
                "row_count": len(df),
                "storage_path": storage_path,
                "columns": list(df.columns)
            },
            metadata={
                "row_count": len(df),
                "columns": len(df.columns),
                "storage_path": storage_path
            }
        )

    except Exception as e:
        context.log.error(f"Error materializing index: {str(e)}")
        raise
    finally:
        session.close()


@asset(
    group_name="feature_engineering",
    description="Computes rule features from materialized index data",
    required_resource_keys={"s3_storage"},
    config_schema={"rule_id": int}
)
def materialized_rule(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Materialize rule features by executing SQL on index data.

    Loads the materialized index data, applies the rule's SQL query
    to compute features, and stores the results.

    Returns metadata about the computed features.
    """
    # Get rule_id from config
    rule_id = context.op_config.get("rule_id")

    if not rule_id:
        raise ValueError("rule_id must be provided in op_config")

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get rule metadata including index_id
        context.log.info(f"Loading rule metadata for rule_id={rule_id}")

        result = session.execute(
            text("""
            SELECT cr.*, ci.conn_id, ci.storage_path as index_storage_path,
                   dc.host, dc.port, dc.database, dc.user, dc.encrypted_password, dc.connection_type
            FROM concept_rules cr
            JOIN concept_indexes ci ON cr.index_id = ci.index_id
            JOIN database_connections dc ON ci.conn_id = dc.conn_id
            WHERE cr.r_id = :rule_id
            """),
            {"rule_id": rule_id}
        )

        rule_row = result.fetchone()

        if not rule_row:
            raise ValueError(f"Rule with ID {rule_id} not found")

        # Load index data from S3 or local path
        index_storage_path = rule_row.index_storage_path
        context.log.info(f"Loading index data from {index_storage_path}")

        # Download from S3 if needed
        if index_storage_path.startswith("s3://"):
            # Parse S3 path
            s3_path_parts = index_storage_path.replace("s3://", "").split("/", 1)
            s3_bucket = s3_path_parts[0]
            s3_key = s3_path_parts[1]

            # Download to temp file
            local_index_path = f"/tmp/index_{rule_row.index_id}.parquet"
            s3_client = context.resources.s3_storage.get_client()
            s3_client.download_file(s3_bucket, s3_key, local_index_path)
            df_index = pd.read_parquet(local_index_path)
        else:
            df_index = pd.read_parquet(index_storage_path)

        context.log.info(f"Loaded {len(df_index)} rows from index")

        # Extract values from index_column for filtering
        index_column = rule_row.index_column or df_index.columns[0]  # Default to first column
        context.log.info(f"Extracting values from column: {index_column}")

        if index_column not in df_index.columns:
            raise ValueError(f"Column '{index_column}' not found in index data. Available columns: {list(df_index.columns)}")

        index_values = df_index[index_column].unique().tolist()
        context.log.info(f"Extracted {len(index_values)} unique values from {index_column}")

        # Format values for SQL IN clause
        # Handle both string and numeric values
        def format_sql_value(val):
            if isinstance(val, str):
                # Escape single quotes in strings
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            else:
                return str(val)

        formatted_values = ", ".join([format_sql_value(v) for v in index_values])

        # Replace :index_values placeholder with formatted values
        rendered_sql = rule_row.sql_query.replace(":index_values", formatted_values)

        # Also support additional template parameters if needed (backward compatibility)
        if rule_row.query_template_params:
            sql_template = Template(rendered_sql)
            rendered_sql = sql_template.render(**rule_row.query_template_params)

        context.log.info(f"Rendered SQL query (first 200 chars): {rendered_sql[:200]}...")
        context.log.info(f"Total query length: {len(rendered_sql)} characters")

        # Connect to external database and execute query
        password = decrypt_password(rule_row.encrypted_password)
        external_conn_str = f"{rule_row.connection_type}://{rule_row.user}:{password}@{rule_row.host}:{rule_row.port}/{rule_row.database}"

        context.log.info(f"Connecting to external database: {rule_row.host}:{rule_row.port}/{rule_row.database}")

        external_engine = create_engine(external_conn_str)
        df_features = pd.read_sql(rendered_sql, external_engine)

        context.log.info(f"Computed features: {len(df_features)} rows, {len(df_features.columns)} columns")

        # Compute column stats
        column_stats = compute_column_stats(df_features)

        # Store results locally
        local_storage_path = get_storage_path("rule", rule_id)
        df_features.to_parquet(local_storage_path, index=False)

        context.log.info(f"Features saved locally to {local_storage_path}")

        # Upload to S3
        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"rules/rule_{rule_id}.parquet"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Features uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # Update rule metadata
        session.execute(
            text("""
            UPDATE concept_rules
            SET is_materialized = true,
                materialized_at = :now,
                row_count = :row_count,
                column_stats = :column_stats,
                storage_path = :storage_path,
                updated_at = :now
            WHERE r_id = :rule_id
            """),
            {
                "rule_id": rule_id,
                "now": datetime.utcnow(),
                "row_count": len(df_features),
                "column_stats": json.dumps(column_stats),
                "storage_path": storage_path
            }
        )

        session.commit()

        context.log.info(f"Rule {rule_id} materialization complete")

        return Output(
            value={
                "rule_id": rule_id,
                "row_count": len(df_features),
                "storage_path": storage_path,
                "columns": list(df_features.columns)
            },
            metadata={
                "row_count": len(df_features),
                "columns": len(df_features.columns),
                "storage_path": storage_path
            }
        )

    except Exception as e:
        context.log.error(f"Error materializing rule: {str(e)}")
        raise
    finally:
        session.close()


@asset(
    group_name="feature_engineering",
    description="Materializes feature data from external database",
    required_resource_keys={"s3_storage"},
    config_schema={"feature_id": int}
)
def materialized_feature(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Materialize a feature by executing its SQL query on the external database.

    Same pattern as materialized_rule: loads index data, extracts unique values,
    replaces :index_values placeholder, executes on external DB, saves as parquet.
    """
    feature_id = context.op_config.get("feature_id")

    if not feature_id:
        raise ValueError("feature_id must be provided in op_config")

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        context.log.info(f"Loading feature metadata for feature_id={feature_id}")

        result = session.execute(
            text("""
            SELECT cf.*, ci.conn_id, ci.storage_path as index_storage_path,
                   dc.host, dc.port, dc.database, dc.user, dc.encrypted_password, dc.connection_type
            FROM concept_features cf
            JOIN concept_indexes ci ON cf.index_id = ci.index_id
            JOIN database_connections dc ON ci.conn_id = dc.conn_id
            WHERE cf.feature_id = :feature_id
            """),
            {"feature_id": feature_id}
        )

        feature_row = result.fetchone()

        if not feature_row:
            raise ValueError(f"Feature with ID {feature_id} not found")

        # Load index data from S3 or local path
        index_storage_path = feature_row.index_storage_path
        context.log.info(f"Loading index data from {index_storage_path}")

        if index_storage_path.startswith("s3://"):
            s3_path_parts = index_storage_path.replace("s3://", "").split("/", 1)
            s3_bucket = s3_path_parts[0]
            s3_key = s3_path_parts[1]
            local_index_path = f"/tmp/index_{feature_row.index_id}.parquet"
            s3_client = context.resources.s3_storage.get_client()
            s3_client.download_file(s3_bucket, s3_key, local_index_path)
            df_index = pd.read_parquet(local_index_path)
        else:
            df_index = pd.read_parquet(index_storage_path)

        context.log.info(f"Loaded {len(df_index)} rows from index")

        # Extract values from index_column for filtering
        index_column = feature_row.index_column or df_index.columns[0]
        context.log.info(f"Extracting values from column: {index_column}")

        if index_column not in df_index.columns:
            raise ValueError(f"Column '{index_column}' not found in index data. Available columns: {list(df_index.columns)}")

        index_values = df_index[index_column].unique().tolist()
        context.log.info(f"Extracted {len(index_values)} unique values from {index_column}")

        # Format values for SQL IN clause
        def format_sql_value(val):
            if isinstance(val, str):
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            else:
                return str(val)

        formatted_values = ", ".join([format_sql_value(v) for v in index_values])

        # Replace :index_values placeholder
        rendered_sql = feature_row.sql_query.replace(":index_values", formatted_values)

        # Support additional template parameters
        if feature_row.query_template_params:
            sql_template = Template(rendered_sql)
            rendered_sql = sql_template.render(**feature_row.query_template_params)

        context.log.info(f"Rendered SQL query (first 200 chars): {rendered_sql[:200]}...")

        # Connect to external database and execute query
        password = decrypt_password(feature_row.encrypted_password)
        external_conn_str = f"{feature_row.connection_type}://{feature_row.user}:{password}@{feature_row.host}:{feature_row.port}/{feature_row.database}"

        context.log.info(f"Connecting to external database: {feature_row.host}:{feature_row.port}/{feature_row.database}")

        external_engine = create_engine(external_conn_str)
        df_features = pd.read_sql(rendered_sql, external_engine)

        context.log.info(f"Computed features: {len(df_features)} rows, {len(df_features.columns)} columns")

        # Compute column stats
        column_stats = compute_column_stats(df_features)

        # Store results locally
        local_storage_path = get_storage_path("feature", feature_id)
        df_features.to_parquet(local_storage_path, index=False)

        context.log.info(f"Features saved locally to {local_storage_path}")

        # Upload to S3
        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"features/feature_{feature_id}.parquet"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Features uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # Update feature metadata
        session.execute(
            text("""
            UPDATE concept_features
            SET is_materialized = true,
                materialized_at = :now,
                row_count = :row_count,
                column_stats = :column_stats,
                storage_path = :storage_path,
                updated_at = :now
            WHERE feature_id = :feature_id
            """),
            {
                "feature_id": feature_id,
                "now": datetime.utcnow(),
                "row_count": len(df_features),
                "column_stats": json.dumps(column_stats),
                "storage_path": storage_path
            }
        )

        session.commit()

        context.log.info(f"Feature {feature_id} materialization complete")

        return Output(
            value={
                "feature_id": feature_id,
                "row_count": len(df_features),
                "storage_path": storage_path,
                "columns": list(df_features.columns)
            },
            metadata={
                "row_count": len(df_features),
                "columns": len(df_features.columns),
                "storage_path": storage_path
            }
        )

    except Exception as e:
        context.log.error(f"Error materializing feature: {str(e)}")
        raise
    finally:
        session.close()


@op(
    config_schema={"dataset_id": int},
    out=DynamicOut()
)
def plan_occupancy_chunks(context: OpExecutionContext):
    """
    Load occupancy dataset metadata and yield one DynamicOutput per time chunk.

    Sets dataset status to RUNNING and emits chunk descriptors that
    fetch_occupancy_chunk will process in parallel.
    """
    dataset_id = context.op_config["dataset_id"]

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        row = session.execute(
            text("SELECT * FROM occupancy_datasets WHERE dataset_id = :id"),
            {"id": dataset_id}
        ).fetchone()

        if not row:
            raise ValueError(f"Occupancy dataset with ID {dataset_id} not found")

        session.execute(
            text("UPDATE occupancy_datasets SET status = 'RUNNING' WHERE dataset_id = :id"),
            {"id": dataset_id}
        )
        session.commit()

        # Query active dates in the subtree to skip empty chunk windows
        tippers_engine = get_tippers_engine()
        with tippers_engine.connect() as tconn:
            active_result = tconn.execute(
                text("""
                WITH RECURSIVE subtree AS (
                    SELECT space_id FROM space WHERE space_id = :root_space_id
                    UNION ALL
                    SELECT s.space_id FROM space s
                    JOIN subtree st ON s.parent_space_id = st.space_id
                )
                SELECT DISTINCT date_trunc('day', start_time)::date AS active_date
                FROM user_ap_trajectory
                WHERE space_id IN (SELECT space_id FROM subtree)
                  AND start_time >= :start_time AND start_time < :end_time
                """),
                {
                    "root_space_id": row.root_space_id,
                    "start_time": row.start_time,
                    "end_time": row.end_time,
                }
            )
            active_dates = {r[0] for r in active_result}

        context.log.info(f"Found {len(active_dates)} active date(s) in range for dataset_id={dataset_id}")

        if not active_dates:
            context.log.info("No data found in range — marking dataset COMPLETED with 0 rows")
            session.execute(
                text("""
                UPDATE occupancy_datasets
                SET status = 'COMPLETED', row_count = 0, completed_at = now()
                WHERE dataset_id = :id
                """),
                {"id": dataset_id}
            )
            session.commit()
            return  # Yield no DynamicOutputs; merge will run with empty list

        i = 0
        if row.chunk_days:
            chunk_delta = timedelta(days=row.chunk_days)
            t = row.start_time
            while t < row.end_time:
                chunk_end = min(t + chunk_delta, row.end_time)
                # Only emit chunks that overlap at least one active date
                chunk_dates = {
                    (t + timedelta(days=d)).date()
                    for d in range(max(1, (chunk_end - t).days))
                }
                if chunk_dates & active_dates:
                    yield DynamicOutput(
                        value={
                            "dataset_id": dataset_id,
                            "chunk_index": i,
                            "chunk_start": t.isoformat(),
                            "chunk_end": chunk_end.isoformat(),
                            "root_space_id": row.root_space_id,
                            "interval_seconds": row.interval_seconds,
                        },
                        mapping_key=f"chunk_{i}"
                    )
                t += chunk_delta
                i += 1
        else:
            yield DynamicOutput(
                value={
                    "dataset_id": dataset_id,
                    "chunk_index": 0,
                    "chunk_start": row.start_time.isoformat(),
                    "chunk_end": row.end_time.isoformat(),
                    "root_space_id": row.root_space_id,
                    "interval_seconds": row.interval_seconds,
                },
                mapping_key="chunk_0"
            )

        context.log.info(f"Planned chunk(s) for dataset_id={dataset_id} (skipping empty windows)")

    except Exception as e:
        context.log.error(f"Error planning occupancy chunks: {str(e)}")
        session.execute(
            text("""
            UPDATE occupancy_datasets
            SET status = 'FAILED', error_message = :error, completed_at = now()
            WHERE dataset_id = :id
            """),
            {"error": str(e), "id": dataset_id}
        )
        session.commit()
        raise
    finally:
        session.close()


@op(required_resource_keys={"s3_storage"})
def fetch_occupancy_chunk(context: OpExecutionContext, chunk: dict) -> str:
    """
    Fetch one occupancy time chunk from the tippers DB and upload to S3.

    Idempotent: if the chunk Parquet already exists in S3, returns its path
    immediately without re-running the query (enables resumable re-runs).
    """
    dataset_id = chunk["dataset_id"]
    chunk_index = chunk["chunk_index"]
    chunk_start = chunk["chunk_start"]
    chunk_end = chunk["chunk_end"]
    root_space_id = chunk["root_space_id"]
    interval_seconds = chunk["interval_seconds"]

    s3_client = context.resources.s3_storage.get_client()
    s3_bucket = context.resources.s3_storage.bucket_name
    s3_key = f"occupancy/chunks/dataset_{dataset_id}/chunk_{chunk_index}.parquet"
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    # Resumability check: skip if chunk already uploaded
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        context.log.info(f"Chunk {chunk_index} already exists at {s3_path}, skipping")
        return s3_path
    except Exception:
        pass  # Object does not exist — proceed with query

    context.log.info(f"Fetching chunk {chunk_index}: {chunk_start} → {chunk_end}")

    tippers_engine = get_tippers_engine()
    params = {
        "root_space_id": root_space_id,
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "interval_seconds": interval_seconds,
    }

    chunk_df = pd.read_sql(text(OCCUPANCY_SQL), tippers_engine, params=params)
    context.log.info(f"Chunk {chunk_index}: {len(chunk_df)} rows")

    local_path = f"/tmp/occupancy_chunk_{dataset_id}_{chunk_index}.parquet"
    chunk_df.to_parquet(local_path, index=False)
    s3_client.upload_file(local_path, s3_bucket, s3_key)
    context.log.info(f"Uploaded chunk {chunk_index} to {s3_path}")

    return s3_path


@op(required_resource_keys={"s3_storage"})
def merge_occupancy_chunks(context: OpExecutionContext, chunk_paths: list) -> None:
    """
    Download all chunk Parquets, concatenate, and store the final dataset.

    Updates DB status to COMPLETED and cleans up intermediate chunk files.
    """
    if not chunk_paths:
        context.log.info("No chunk paths provided — dataset was already handled by planner (0 rows)")
        return

    match = re.search(r"dataset_(\d+)", chunk_paths[0])
    if not match:
        raise ValueError(f"Could not parse dataset_id from path: {chunk_paths[0]}")
    dataset_id = int(match.group(1))

    context.log.info(f"Merging {len(chunk_paths)} chunks for dataset_id={dataset_id}")

    s3_client = context.resources.s3_storage.get_client()
    s3_bucket = context.resources.s3_storage.bucket_name

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        dfs = []
        for i, s3_path in enumerate(chunk_paths):
            s3_key = s3_path.replace(f"s3://{s3_bucket}/", "")
            local_path = f"/tmp/merge_chunk_{dataset_id}_{i}.parquet"
            s3_client.download_file(s3_bucket, s3_key, local_path)
            dfs.append(pd.read_parquet(local_path))

        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(
            columns=["space_id", "interval_begin_time", "number_connections"]
        )

        context.log.info(f"Total rows after merge: {len(df)}")

        column_stats = compute_column_stats(df)

        final_s3_key = f"occupancy/datasets/{dataset_id}/data.parquet"
        final_local_path = f"/tmp/occupancy_{dataset_id}_final.parquet"
        df.to_parquet(final_local_path, index=False)
        s3_client.upload_file(final_local_path, s3_bucket, final_s3_key)
        storage_path = f"s3://{s3_bucket}/{final_s3_key}"
        context.log.info(f"Uploaded final dataset to {storage_path}")

        # Clean up intermediate chunk files
        for s3_path in chunk_paths:
            chunk_key = s3_path.replace(f"s3://{s3_bucket}/", "")
            try:
                s3_client.delete_object(Bucket=s3_bucket, Key=chunk_key)
            except Exception as del_err:
                context.log.warning(f"Failed to delete chunk {chunk_key}: {del_err}")

        session.execute(
            text("""
            UPDATE occupancy_datasets
            SET status = 'COMPLETED',
                storage_path = :path,
                row_count = :rc,
                column_stats = CAST(:cs AS jsonb),
                completed_at = now()
            WHERE dataset_id = :id
            """),
            {
                "path": storage_path,
                "rc": len(df),
                "cs": json.dumps(column_stats),
                "id": dataset_id,
            }
        )
        session.commit()

        context.log.info(f"Occupancy dataset {dataset_id} merge complete")

    except Exception as e:
        context.log.error(f"Error merging occupancy chunks: {str(e)}")
        session.execute(
            text("""
            UPDATE occupancy_datasets
            SET status = 'FAILED', error_message = :error, completed_at = now()
            WHERE dataset_id = :id AND status != 'COMPLETED'
            """),
            {"error": str(e), "id": dataset_id}
        )
        session.commit()
        raise
    finally:
        session.close()


@graph
def occupancy_dataset_graph():
    chunk_paths = plan_occupancy_chunks().map(fetch_occupancy_chunk)
    merge_occupancy_chunks(chunk_paths.collect())


@run_failure_sensor
def occupancy_dataset_failure_sensor(context: RunFailureSensorContext):
    """Mark occupancy dataset as FAILED if the Dagster run fails before merge completes."""
    if context.dagster_run.job_name != "occupancy_dataset_job":
        return

    try:
        dataset_id = context.dagster_run.run_config["ops"]["plan_occupancy_chunks"]["config"]["dataset_id"]
    except (KeyError, TypeError):
        return

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.execute(
            text("""
            UPDATE occupancy_datasets
            SET status = 'FAILED',
                error_message = :error,
                completed_at = now()
            WHERE dataset_id = :id AND status != 'COMPLETED'
            """),
            {
                "error": f"Dagster run {context.dagster_run.run_id} failed",
                "id": dataset_id,
            }
        )
        session.commit()
    finally:
        session.close()


def apply_custom_lf(df: pd.DataFrame, lf_config: dict, valid_cv_ids: set, cv_id_to_index: dict,
                    cv_name_to_id: dict, context) -> np.ndarray:
    """
    Execute custom Python labeling function.

    Safely executes user-provided code against feature DataFrame.
    Return values are remapped from cv_ids to 0-indexed class labels using cv_id_to_index.
    -1 (ABSTAIN) is preserved as-is.

    After exec'ing user code, concept value constants (e.g., STATIC, LAPTOP, PHONE)
    are overridden in the globals with the actual cv_ids from the database, so the
    labeling function sees correct values at call time regardless of what the user
    hardcoded.

    Args:
        df: Feature DataFrame
        lf_config: LF configuration with code and allowed_imports
        valid_cv_ids: Set of valid cv_ids (ints) the LF may return
        cv_id_to_index: Mapping from cv_id (int) to 0-indexed class label
        cv_name_to_id: Mapping from uppercased concept value name to cv_id (int)
        context: Dagster execution context
    """
    code = lf_config.get("code", "")
    allowed_imports = lf_config.get("allowed_imports", [])

    # Build safe execution environment
    safe_globals = {
        "__builtins__": {
            "abs": abs, "min": min, "max": max, "len": len,
            "int": int, "float": float, "str": str, "bool": bool,
            "sum": sum, "round": round, "range": range
        },
        "np": np,
        "pd": pd
    }

    # Add allowed imports
    for module_name in allowed_imports:
        if module_name == "math":
            import math
            safe_globals["math"] = math
        elif module_name == "re":
            import re
            safe_globals["re"] = re
        elif module_name == "datetime":
            from datetime import datetime
            safe_globals["datetime"] = datetime
        elif module_name == "statistics":
            import statistics
            safe_globals["statistics"] = statistics

    # Compile the function
    exec(code, safe_globals)

    # Override concept value constants with actual cv_ids from DB.
    # User code may define e.g. PHONE = 3, but actual cv_id is 6.
    # Since the function references these as globals, overriding after exec
    # ensures the function sees the correct values at call time.
    safe_globals["ABSTAIN"] = -1
    safe_globals.update(cv_name_to_id)

    # Get the labeling function (should be named 'labeling_function')
    labeling_function = safe_globals.get("labeling_function")

    if not labeling_function:
        raise ValueError("Custom code must define a function named 'labeling_function'")

    # Apply to each row
    labels = np.full(len(df), -1, dtype=int)
    for idx, row in df.iterrows():
        try:
            label = int(labeling_function(row))
            if label == -1:
                labels[idx] = -1  # Abstain
            elif label in valid_cv_ids:
                labels[idx] = cv_id_to_index[label]  # Remap cv_id to 0-indexed
            else:
                context.log.warning(f"LF returned invalid label {label} for row {idx}, abstaining")
                labels[idx] = -1
        except Exception as e:
            context.log.warning(f"Error applying LF to row {idx}: {e}")
            labels[idx] = -1  # Abstain on error

    return labels


def load_parquet_from_storage(storage_path: str, s3_resource, asset_label: str, asset_id: int) -> pd.DataFrame:
    """Load a parquet file from S3 or local storage."""
    if storage_path.startswith("s3://"):
        s3_path_parts = storage_path.replace("s3://", "").split("/", 1)
        s3_bucket = s3_path_parts[0]
        s3_key = s3_path_parts[1]
        local_path = f"/tmp/{asset_label}_{asset_id}.parquet"
        s3_client = s3_resource.get_client()
        s3_client.download_file(s3_bucket, s3_key, local_path)
        return pd.read_parquet(local_path)
    else:
        return pd.read_parquet(storage_path)


def load_rule_features(rule_id: int, session, s3_resource, context) -> pd.DataFrame:
    """Load materialized rule features from storage."""
    result = session.execute(
        text("SELECT storage_path FROM concept_rules WHERE r_id = :rule_id"),
        {"rule_id": rule_id}
    )
    row = result.fetchone()
    if not row or not row.storage_path:
        raise ValueError(f"Rule {rule_id} has no materialized data")

    storage_path = row.storage_path

    if storage_path.startswith("s3://"):
        s3_path_parts = storage_path.replace("s3://", "").split("/", 1)
        s3_bucket = s3_path_parts[0]
        s3_key = s3_path_parts[1]
        local_path = f"/tmp/rule_{rule_id}.parquet"
        s3_client = s3_resource.get_client()
        s3_client.download_file(s3_bucket, s3_key, local_path)
        return pd.read_parquet(local_path)
    else:
        return pd.read_parquet(storage_path)


@asset(
    group_name="weak_supervision",
    description="Trains Snorkel label model and generates labels",
    required_resource_keys={"s3_storage"},
    config_schema={"job_id": int}
)
def snorkel_training(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Train Snorkel label model and generate probabilistic labels.

    For each selected LF, loads the associated rule's materialized parquet,
    joins all rule feature DataFrames, applies labeling functions with
    cv_id-to-index remapping, and trains Snorkel's LabelModel.

    Returns metadata about the labeled dataset.
    """
    job_id = context.op_config.get("job_id")

    if not job_id:
        raise ValueError("job_id must be provided in op_config")

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get Snorkel job metadata
        context.log.info(f"Loading Snorkel job metadata for job_id={job_id}")

        result = session.execute(
            text("SELECT * FROM snorkel_jobs WHERE job_id = :job_id"),
            {"job_id": job_id}
        )
        job_row = result.fetchone()

        if not job_row:
            raise ValueError(f"Snorkel job with ID {job_id} not found")

        # Get labeling functions
        lf_ids = job_row.lf_ids
        if not lf_ids:
            raise ValueError("No labeling functions specified")

        context.log.info(f"Loading {len(lf_ids)} labeling functions")

        lf_result = session.execute(
            text("""
            SELECT * FROM labeling_functions
            WHERE lf_id = ANY(:lf_ids)
            AND is_active = true
            ORDER BY lf_id
            """),
            {"lf_ids": lf_ids}
        )
        lfs = lf_result.fetchall()

        context.log.info(f"Loaded {len(lfs)} active labeling functions")

        # Build cv_id-to-index mapping from the union of all LFs' applicable_cv_ids
        # Cast to int for type consistency (raw SQL may return strings)
        all_cv_ids = set()
        for lf in lfs:
            all_cv_ids.update(int(cv) for cv in lf.applicable_cv_ids)

        sorted_cv_ids = sorted(all_cv_ids)
        cv_id_to_index = {cv_id: idx for idx, cv_id in enumerate(sorted_cv_ids)}
        index_to_cv_id = {idx: cv_id for cv_id, idx in cv_id_to_index.items()}
        cardinality = len(sorted_cv_ids)

        context.log.info(f"CV-ID-to-index mapping: {cv_id_to_index} (cardinality={cardinality})")

        # Look up concept value names to inject correct constants into LF execution
        cv_names_result = session.execute(
            text("SELECT cv_id, name FROM concept_values WHERE cv_id = ANY(:cv_ids)"),
            {"cv_ids": list(all_cv_ids)}
        )
        cv_name_to_id = {}
        cv_id_to_name = {}
        for row in cv_names_result.fetchall():
            cv_id_int = int(row.cv_id)
            upper_name = row.name.upper().replace(" ", "_")
            cv_name_to_id[upper_name] = cv_id_int
            cv_id_to_name[cv_id_int] = row.name

        context.log.info(f"Concept value constants: {cv_name_to_id}")

        # Load rule feature DataFrames and look up index columns
        s3_resource = context.resources.s3_storage
        rule_dfs = {}
        rule_index_cols = {}
        for lf in lfs:
            if lf.rule_id not in rule_dfs:
                context.log.info(f"Loading features for rule_id={lf.rule_id}")
                rule_dfs[lf.rule_id] = load_rule_features(lf.rule_id, session, s3_resource, context)

                # Look up the index_column for this rule
                rule_meta = session.execute(
                    text("SELECT index_column FROM concept_rules WHERE r_id = :rule_id"),
                    {"rule_id": lf.rule_id}
                ).fetchone()
                idx_col = rule_meta.index_column if rule_meta and rule_meta.index_column else rule_dfs[lf.rule_id].columns[0]
                rule_index_cols[lf.rule_id] = idx_col

        # Join all rule feature DataFrames on the index column (outer join)
        rule_ids = list(rule_dfs.keys())
        first_rid = rule_ids[0]
        idx_col = rule_index_cols[first_rid]
        df_features = rule_dfs[first_rid].set_index(idx_col)

        for rid in rule_ids[1:]:
            idx_col = rule_index_cols[rid]
            other_df = rule_dfs[rid].set_index(idx_col)
            df_features = df_features.join(other_df, how="outer", rsuffix=f"_rule{rid}")

        df_features = df_features.reset_index()

        context.log.info(f"Combined feature DataFrame: {len(df_features)} rows, {len(df_features.columns)} columns")

        # Apply labeling functions to create label matrix
        n_samples = len(df_features)
        n_lfs = len(lfs)
        L = np.full((n_samples, n_lfs), -1, dtype=int)  # Label matrix, default abstain

        for i, lf in enumerate(lfs):
            context.log.info(f"Applying LF {i+1}/{n_lfs}: {lf.name}")
            valid_cv_ids = set(int(cv) for cv in lf.applicable_cv_ids)

            try:
                # Apply LF on the combined DataFrame (columns from all rules are present)
                L[:, i] = apply_custom_lf(df_features, lf.lf_config, valid_cv_ids, cv_id_to_index,
                                          cv_name_to_id, context)
            except Exception as e:
                context.log.error(f"Error applying LF {lf.name}: {str(e)}")
                L[:, i] = -1

        context.log.info("Label matrix created")

        # ---- Compute LF summary stats from label matrix ----
        lf_summary = []
        for i, lf in enumerate(lfs):
            col = L[:, i]
            votes = col != -1
            n_votes = int(votes.sum())
            coverage_i = n_votes / n_samples if n_samples > 0 else 0.0

            # Polarity: set of unique non-abstain labels this LF emits (as 0-indexed)
            polarity = sorted(set(col[votes].tolist()))

            # Overlaps: fraction of samples where this LF votes AND at least one other LF also votes
            other_votes = np.zeros(n_samples, dtype=bool)
            for j in range(n_lfs):
                if j != i:
                    other_votes |= (L[:, j] != -1)
            overlap_count = int((votes & other_votes).sum())
            overlaps_i = overlap_count / n_samples if n_samples > 0 else 0.0

            # Conflicts: fraction of samples where this LF disagrees with at least one other LF
            conflict_count = 0
            for j in range(n_lfs):
                if j != i:
                    both_vote = votes & (L[:, j] != -1)
                    disagree = L[both_vote, i] != L[both_vote, j]
                    conflict_count += int(disagree.sum())
            conflicts_i = conflict_count / n_samples if n_samples > 0 else 0.0

            lf_summary.append({
                "lf_id": int(lf.lf_id),
                "name": lf.name,
                "polarity": polarity,
                "coverage": round(coverage_i, 4),
                "overlaps": round(overlaps_i, 4),
                "conflicts": round(conflicts_i, 4),
                "n_votes": n_votes,
            })

        context.log.info(f"LF summary computed for {n_lfs} LFs")

        # ---- Compute class distribution from label matrix (majority vote) ----
        # For each sample, take the majority vote across LFs (ignoring abstains)
        majority_labels = []
        for row_idx in range(n_samples):
            row_votes = L[row_idx, :]
            non_abstain = row_votes[row_votes != -1]
            if len(non_abstain) == 0:
                majority_labels.append(-1)
            else:
                counts = np.bincount(non_abstain, minlength=cardinality)
                majority_labels.append(int(np.argmax(counts)))
        majority_labels = np.array(majority_labels)

        class_distribution = {}
        for class_idx in range(cardinality):
            cv_id = index_to_cv_id[class_idx]
            label_name = cv_id_to_name.get(cv_id, f"class_{class_idx}")
            class_distribution[label_name] = int((majority_labels == class_idx).sum())
        class_distribution["ABSTAIN"] = int((majority_labels == -1).sum())

        # Overall stats
        total_coverage = float((majority_labels != -1).sum()) / n_samples if n_samples > 0 else 0.0
        mean_coverage = float(np.mean([s["coverage"] for s in lf_summary])) if lf_summary else 0.0
        mean_overlaps = float(np.mean([s["overlaps"] for s in lf_summary])) if lf_summary else 0.0
        mean_conflicts = float(np.mean([s["conflicts"] for s in lf_summary])) if lf_summary else 0.0

        context.log.info(f"Class distribution: {class_distribution}")

        # Read output_type before the try/except so it's always available
        output_type = job_row.output_type or "softmax"

        # Train Snorkel LabelModel
        try:
            from snorkel.labeling import LabelModel

            label_model = LabelModel(cardinality=cardinality, verbose=True)

            config = job_row.config or {}
            epochs = config.get("epochs", 100)
            lr = config.get("lr", 0.01)

            context.log.info(f"Training LabelModel (epochs={epochs}, lr={lr}, cardinality={cardinality})")

            label_model.fit(
                L_train=L,
                n_epochs=epochs,
                lr=lr,
                log_freq=10
            )

            context.log.info("LabelModel training complete")

            # Get LF weights from trained model
            lf_weights = label_model.get_weights()
            for i, summary in enumerate(lf_summary):
                summary["learned_weight"] = round(float(lf_weights[i]), 4) if i < len(lf_weights) else None

            # Generate predictions
            if output_type == "hard_labels":
                predictions = label_model.predict(L)
                prediction_data = {
                    "labels": predictions.tolist(),
                    "sample_ids": list(range(len(predictions))),
                }
                # Class distribution from model predictions
                model_class_dist = {}
                for class_idx in range(cardinality):
                    cv_id = index_to_cv_id[class_idx]
                    label_name = cv_id_to_name.get(cv_id, f"class_{class_idx}")
                    model_class_dist[label_name] = int((predictions == class_idx).sum())
                model_class_dist["ABSTAIN"] = int((predictions == -1).sum())
            else:  # softmax
                probs = label_model.predict_proba(L)
                predictions = np.argmax(probs, axis=1)
                prediction_data = {
                    "probabilities": probs.tolist(),
                    "sample_ids": list(range(len(probs))),
                }
                # Class distribution from model predictions (argmax of probabilities)
                model_class_dist = {}
                for class_idx in range(cardinality):
                    cv_id = index_to_cv_id[class_idx]
                    label_name = cv_id_to_name.get(cv_id, f"class_{class_idx}")
                    model_class_dist[label_name] = int((predictions == class_idx).sum())

            # Build full result data
            result_data = {
                **prediction_data,
                "output_type": output_type,
                "cv_id_to_index": {str(k): v for k, v in cv_id_to_index.items()},
                "index_to_cv_id": {str(k): v for k, v in index_to_cv_id.items()},
                "cv_id_to_name": {str(k): v for k, v in cv_id_to_name.items()},
                "lf_summary": lf_summary,
                "label_matrix_class_distribution": class_distribution,
                "model_class_distribution": model_class_dist,
                "overall_stats": {
                    "n_samples": n_samples,
                    "n_lfs": n_lfs,
                    "cardinality": cardinality,
                    "total_coverage": round(total_coverage, 4),
                    "mean_lf_coverage": round(mean_coverage, 4),
                    "mean_lf_overlaps": round(mean_overlaps, 4),
                    "mean_lf_conflicts": round(mean_conflicts, 4),
                },
            }

            # Update LF performance metrics in DB
            for i, lf in enumerate(lfs):
                session.execute(
                    text("""
                    UPDATE labeling_functions
                    SET estimated_accuracy = :accuracy,
                        coverage = :coverage,
                        conflicts = :conflicts,
                        updated_at = :now
                    WHERE lf_id = :lf_id
                    """),
                    {
                        "lf_id": lf.lf_id,
                        "accuracy": float(lf_weights[i]) if i < len(lf_weights) else None,
                        "coverage": lf_summary[i]["coverage"],
                        "conflicts": lf_summary[i]["n_votes"],
                        "now": datetime.utcnow()
                    }
                )

            session.commit()
            context.log.info("LF metrics updated")

        except ImportError:
            context.log.warning("Snorkel not installed. Using majority vote predictions.")
            result_data = {
                "labels": majority_labels.tolist(),
                "sample_ids": list(range(n_samples)),
                "output_type": "hard_labels",
                "cv_id_to_index": {str(k): v for k, v in cv_id_to_index.items()},
                "index_to_cv_id": {str(k): v for k, v in index_to_cv_id.items()},
                "cv_id_to_name": {str(k): v for k, v in cv_id_to_name.items()},
                "lf_summary": lf_summary,
                "label_matrix_class_distribution": class_distribution,
                "model_class_distribution": class_distribution,
                "overall_stats": {
                    "n_samples": n_samples,
                    "n_lfs": n_lfs,
                    "cardinality": cardinality,
                    "total_coverage": round(total_coverage, 4),
                    "mean_lf_coverage": round(mean_coverage, 4),
                    "mean_lf_overlaps": round(mean_overlaps, 4),
                    "mean_lf_conflicts": round(mean_conflicts, 4),
                },
            }

        # Store results locally
        local_storage_path = get_storage_path("snorkel_job", job_id)
        local_storage_path = local_storage_path.replace('.parquet', '.json')

        with open(local_storage_path, 'w') as f:
            json.dump(result_data, f)

        context.log.info(f"Results saved locally to {local_storage_path}")

        # Upload to S3
        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"snorkel_jobs/job_{job_id}.json"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Results uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # Update job status
        session.execute(
            text("""
            UPDATE snorkel_jobs
            SET status = 'COMPLETED',
                result_path = :storage_path,
                completed_at = :now
            WHERE job_id = :job_id
            """),
            {
                "job_id": job_id,
                "storage_path": storage_path,
                "now": datetime.utcnow()
            }
        )

        session.commit()

        context.log.info(f"Snorkel job {job_id} complete")

        return Output(
            value={
                "job_id": job_id,
                "n_samples": n_samples,
                "n_lfs": n_lfs,
                "storage_path": storage_path
            },
            metadata={
                "n_samples": n_samples,
                "n_lfs": n_lfs,
                "output_type": output_type,
                "storage_path": storage_path
            }
        )

    except Exception as e:
        context.log.error(f"Error in Snorkel training: {str(e)}")

        session.execute(
            text("""
            UPDATE snorkel_jobs
            SET status = 'FAILED',
                error_message = :error,
                completed_at = :now
            WHERE job_id = :job_id
            """),
            {
                "job_id": job_id,
                "error": str(e),
                "now": datetime.utcnow()
            }
        )

        session.commit()
        raise
    finally:
        session.close()


@asset(
    group_name="classification",
    description="Trains baseline classifiers on Snorkel-labeled data with feature joining",
    required_resource_keys={"s3_storage"},
    config_schema={"job_id": int}
)
def classifier_training(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Train a bank of classifiers on Snorkel-labeled data.

    Steps:
    1. Load classifier job metadata
    2. Load Snorkel results (probabilistic labels)
    3. Filter by confidence threshold
    4. Load and join feature data
    5. Train classifiers sequentially (memory-safe)
    6. Save results
    """
    job_id = context.op_config.get("job_id")

    if not job_id:
        raise ValueError("job_id must be provided in op_config")

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Load classifier job metadata
        context.log.info(f"Loading classifier job metadata for job_id={job_id}")

        result = session.execute(
            text("SELECT * FROM classifier_jobs WHERE job_id = :job_id"),
            {"job_id": job_id}
        )
        job_row = result.fetchone()

        if not job_row:
            raise ValueError(f"Classifier job with ID {job_id} not found")

        config = job_row.config
        snorkel_job_id = job_row.snorkel_job_id
        feature_ids = [int(fid) for fid in job_row.feature_ids]

        threshold_method = config.get("threshold_method", "max_confidence")
        threshold_value = config.get("threshold_value", 0.7)
        min_labels_per_class = config.get("min_labels_per_class", 10)
        imbalance_factor = config.get("imbalance_factor", 3.0)
        test_size = config.get("test_size", 0.2)
        random_state = config.get("random_state", 42)
        n_estimators = config.get("n_estimators", 100)
        max_depth = config.get("max_depth", None)

        # Update status to RUNNING
        session.execute(
            text("UPDATE classifier_jobs SET status = 'RUNNING' WHERE job_id = :job_id"),
            {"job_id": job_id}
        )
        session.commit()

        # 2. Load Snorkel results
        context.log.info(f"Loading Snorkel results from job {snorkel_job_id}")

        snorkel_result = session.execute(
            text("SELECT * FROM snorkel_jobs WHERE job_id = :job_id"),
            {"job_id": snorkel_job_id}
        )
        snorkel_row = snorkel_result.fetchone()

        if not snorkel_row:
            raise ValueError(f"Snorkel job {snorkel_job_id} not found")

        if not snorkel_row.result_path:
            raise ValueError(f"Snorkel job {snorkel_job_id} has no result_path")

        # Load Snorkel results JSON
        s3_resource = context.resources.s3_storage
        snorkel_result_path = snorkel_row.result_path

        if snorkel_result_path.startswith("s3://"):
            s3_path_parts = snorkel_result_path.replace("s3://", "").split("/", 1)
            s3_bucket = s3_path_parts[0]
            s3_key = s3_path_parts[1]
            local_snorkel_path = f"/tmp/snorkel_result_{snorkel_job_id}.json"
            s3_client = s3_resource.get_client()
            s3_client.download_file(s3_bucket, s3_key, local_snorkel_path)
            with open(local_snorkel_path, 'r') as f:
                snorkel_data = json.load(f)
        else:
            with open(snorkel_result_path, 'r') as f:
                snorkel_data = json.load(f)

        # 3. Reconstruct labeled DataFrame
        output_type = snorkel_data.get("output_type", "softmax")
        sample_ids = snorkel_data.get("sample_ids", [])
        index_to_cv_id = {int(k): v for k, v in snorkel_data.get("index_to_cv_id", {}).items()}
        cv_id_to_name = {int(k): v for k, v in snorkel_data.get("cv_id_to_name", {}).items()}

        # Load index data to get the index column values
        index_id = snorkel_row.index_id
        index_meta = session.execute(
            text("SELECT storage_path, index_id FROM concept_indexes WHERE index_id = :index_id"),
            {"index_id": index_id}
        )
        index_row = index_meta.fetchone()
        if not index_row or not index_row.storage_path:
            raise ValueError(f"Index {index_id} has no materialized data")

        df_index = load_parquet_from_storage(index_row.storage_path, s3_resource, "index", index_id)

        if output_type == "softmax":
            probs = np.array(snorkel_data.get("probabilities", []))
            if len(probs) == 0:
                raise ValueError("No probabilities found in Snorkel results")

            y_hat = np.argmax(probs, axis=1)

            # Compute confidence
            if threshold_method == "entropy":
                # Entropy-based confidence: 1 - (H / H_max)
                eps = 1e-10
                entropy = -np.sum(probs * np.log(probs + eps), axis=1)
                max_entropy = np.log(probs.shape[1])
                confidence = 1.0 - (entropy / max_entropy)
            else:  # max_confidence
                confidence = np.max(probs, axis=1)
        else:
            labels = np.array(snorkel_data.get("labels", []))
            if len(labels) == 0:
                raise ValueError("No labels found in Snorkel results")
            y_hat = labels
            confidence = np.ones(len(labels))  # Hard labels get full confidence

        n_samples_before = len(y_hat)
        context.log.info(f"Loaded {n_samples_before} samples from Snorkel results")

        # Compute stats before filtering
        mean_confidence_before = float(np.mean(confidence))

        # 4. Filter by confidence threshold
        mask = confidence >= threshold_value
        # Also remove abstains (label == -1)
        mask &= (y_hat != -1)

        y_filtered = y_hat[mask]
        confidence_filtered = confidence[mask]
        sample_ids_filtered = np.array(sample_ids)[mask]

        context.log.info(f"After confidence filter (>= {threshold_value}): {len(y_filtered)} samples")

        # Ensure min_labels_per_class — lower threshold incrementally if needed
        unique_classes, class_counts = np.unique(y_filtered, return_counts=True)
        classes_below_min = [c for c, cnt in zip(unique_classes, class_counts) if cnt < min_labels_per_class]

        if classes_below_min and threshold_value > 0.1:
            context.log.warning(
                f"Classes {classes_below_min} have fewer than {min_labels_per_class} samples. "
                f"Attempting to lower threshold."
            )
            adjusted_threshold = threshold_value
            while classes_below_min and adjusted_threshold > 0.1:
                adjusted_threshold -= 0.05
                mask = confidence >= adjusted_threshold
                mask &= (y_hat != -1)
                y_filtered = y_hat[mask]
                confidence_filtered = confidence[mask]
                sample_ids_filtered = np.array(sample_ids)[mask]
                unique_classes, class_counts = np.unique(y_filtered, return_counts=True)
                classes_below_min = [c for c, cnt in zip(unique_classes, class_counts) if cnt < min_labels_per_class]

            if adjusted_threshold < threshold_value:
                context.log.info(f"Threshold adjusted from {threshold_value} to {adjusted_threshold}")

        # Cap class sizes by imbalance_factor * min_class_size
        unique_classes, class_counts = np.unique(y_filtered, return_counts=True)
        if len(unique_classes) > 0:
            min_class_size = int(class_counts.min())
            max_allowed = int(imbalance_factor * min_class_size)

            balanced_indices = []
            for cls in unique_classes:
                cls_indices = np.where(y_filtered == cls)[0]
                if len(cls_indices) > max_allowed:
                    rng = np.random.RandomState(random_state)
                    cls_indices = rng.choice(cls_indices, size=max_allowed, replace=False)
                balanced_indices.extend(cls_indices)

            balanced_indices = sorted(balanced_indices)
            y_filtered = y_filtered[balanced_indices]
            confidence_filtered = confidence_filtered[balanced_indices]
            sample_ids_filtered = sample_ids_filtered[balanced_indices]

        n_samples_after = len(y_filtered)
        mean_confidence_after = float(np.mean(confidence_filtered)) if n_samples_after > 0 else 0.0

        # Build class distribution after filter
        class_dist_after = {}
        unique_classes_after, counts_after = np.unique(y_filtered, return_counts=True)
        for cls, cnt in zip(unique_classes_after, counts_after):
            cv_id = index_to_cv_id.get(int(cls), int(cls))
            label_name = cv_id_to_name.get(cv_id, f"class_{cls}")
            class_dist_after[label_name] = int(cnt)

        filtering_stats = {
            "samples_before_filter": n_samples_before,
            "samples_after_filter": n_samples_after,
            "confidence_before_filter": round(mean_confidence_before, 4),
            "confidence_after_filter": round(mean_confidence_after, 4),
            "filter_retention_rate": round(n_samples_after / n_samples_before, 4) if n_samples_before > 0 else 0.0,
            "class_distribution_after_filter": class_dist_after,
        }

        context.log.info(f"Filtering stats: {filtering_stats}")

        if n_samples_after < 2:
            raise ValueError(f"Only {n_samples_after} samples remain after filtering. Need at least 2.")

        # 5. Load and join features
        context.log.info(f"Loading {len(feature_ids)} features")

        feature_dfs = []
        feature_index_cols = []
        for fid in feature_ids:
            feat_meta = session.execute(
                text("SELECT storage_path, index_column FROM concept_features WHERE feature_id = :fid"),
                {"fid": fid}
            )
            feat_row = feat_meta.fetchone()
            if not feat_row or not feat_row.storage_path:
                raise ValueError(f"Feature {fid} has no materialized data")

            df_feat = load_parquet_from_storage(feat_row.storage_path, s3_resource, "feature", fid)
            idx_col = feat_row.index_column or df_feat.columns[0]
            feature_dfs.append(df_feat)
            feature_index_cols.append(idx_col)
            context.log.info(f"Loaded feature {fid}: {len(df_feat)} rows, {len(df_feat.columns)} columns")

        # Build training DataFrame by joining features
        # Start with index data (filtered to sample_ids_filtered)
        df_train = df_index.iloc[sample_ids_filtered].copy().reset_index(drop=True)
        join_col = df_index.columns[0]  # Use first column of index as join key

        for i, (df_feat, idx_col) in enumerate(zip(feature_dfs, feature_index_cols)):
            df_train = df_train.merge(df_feat, left_on=join_col, right_on=idx_col, how="left", suffixes=("", f"_feat{feature_ids[i]}"))

        # Drop the join column and any non-numeric columns to get X
        # Keep only numeric columns as features
        X = df_train.select_dtypes(include=[np.number]).copy()
        # Remove any columns that are all NaN
        X = X.dropna(axis=1, how="all")
        # Fill remaining NaN with 0
        X = X.fillna(0)

        # Map y to class names for interpretability
        y = pd.Series(y_filtered).map(lambda c: cv_id_to_name.get(index_to_cv_id.get(int(c), int(c)), f"class_{c}"))

        context.log.info(f"Training data: X shape={X.shape}, y shape={y.shape}")
        context.log.info(f"Classes: {y.value_counts().to_dict()}")

        if len(X) == 0 or X.shape[1] == 0:
            raise ValueError(f"No features available for training. X shape: {X.shape}")

        # 6. Train classifiers (one at a time to avoid OOM)
        import gc
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score
        from sklearn.preprocessing import StandardScaler
        from sklearn.linear_model import LogisticRegression, SGDClassifier, RidgeClassifier
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.ensemble import (
            RandomForestClassifier, GradientBoostingClassifier,
            AdaBoostClassifier, ExtraTreesClassifier,
        )
        from sklearn.svm import LinearSVC
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.naive_bayes import GaussianNB
        from sklearn.discriminant_analysis import LinearDiscriminantAnalysis

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )

        context.log.info(f"Train/test split: {len(X_train)} train, {len(X_test)} test")

        classifiers = [
            ("LogisticRegression", LogisticRegression(max_iter=1000, random_state=random_state)),
            ("RidgeClassifier", RidgeClassifier(random_state=random_state)),
            ("SGDClassifier", SGDClassifier(random_state=random_state)),
            ("DecisionTree", DecisionTreeClassifier(max_depth=max_depth, random_state=random_state)),
            ("RandomForest", RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state)),
            ("ExtraTrees", ExtraTreesClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state)),
            ("GradientBoosting", GradientBoostingClassifier(n_estimators=n_estimators, max_depth=max_depth or 3, random_state=random_state)),
            ("AdaBoost", AdaBoostClassifier(n_estimators=min(n_estimators, 50), random_state=random_state)),
            ("KNeighbors", KNeighborsClassifier()),
            ("LinearSVC", LinearSVC(max_iter=2000, random_state=random_state)),
            ("GaussianNB", GaussianNB()),
            ("LDA", LinearDiscriminantAnalysis()),
        ]

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        model_scores = []
        for name, estimator in classifiers:
            try:
                estimator.fit(X_train_scaled, y_train)
                y_pred = estimator.predict(X_test_scaled)
                model_scores.append({
                    "Model": name,
                    "Accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
                    "Balanced Accuracy": round(float(balanced_accuracy_score(y_test, y_pred)), 4),
                    "F1 Score": round(float(f1_score(y_test, y_pred, average="weighted")), 4),
                })
                context.log.info(f"  {name}: accuracy={model_scores[-1]['Accuracy']}")
            except Exception as model_err:
                context.log.warning(f"  {name} failed: {model_err}")
            finally:
                del estimator
                gc.collect()

        # Sort by accuracy descending
        model_scores.sort(key=lambda m: m["Accuracy"], reverse=True)

        context.log.info(f"Trained {len(model_scores)} models")

        # 7. Save results
        result_data = {
            "filtering_stats": filtering_stats,
            "model_scores": model_scores,
            "num_models_trained": len(model_scores),
            "config_used": config,
        }

        local_storage_path = get_storage_path("classifier_job", job_id)
        local_storage_path = local_storage_path.replace('.parquet', '.json')

        with open(local_storage_path, 'w') as f:
            json.dump(result_data, f, default=str)

        context.log.info(f"Results saved locally to {local_storage_path}")

        # Upload to S3
        s3_client = s3_resource.get_client()
        s3_bucket = s3_resource.bucket_name
        s3_key = f"classifier_jobs/job_{job_id}.json"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Results uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # 8. Update job status
        session.execute(
            text("""
            UPDATE classifier_jobs
            SET status = 'COMPLETED',
                result_path = :storage_path,
                completed_at = :now
            WHERE job_id = :job_id
            """),
            {
                "job_id": job_id,
                "storage_path": storage_path,
                "now": datetime.utcnow()
            }
        )

        session.commit()

        context.log.info(f"Classifier job {job_id} complete")

        return Output(
            value={
                "job_id": job_id,
                "num_models_trained": len(model_scores),
                "storage_path": storage_path
            },
            metadata={
                "num_models_trained": len(model_scores),
                "samples_after_filter": n_samples_after,
                "storage_path": storage_path
            }
        )

    except Exception as e:
        context.log.error(f"Error in classifier training: {str(e)}")

        session.execute(
            text("""
            UPDATE classifier_jobs
            SET status = 'FAILED',
                error_message = :error,
                completed_at = :now
            WHERE job_id = :job_id
            """),
            {
                "job_id": job_id,
                "error": str(e),
                "now": datetime.utcnow()
            }
        )

        session.commit()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Per-space chunk ops (bottom-up occupancy materialization)
# ---------------------------------------------------------------------------

SOURCE_CHUNK_SQL = """
WITH bins AS (
    SELECT to_timestamp(gs)::timestamp AS bin_start
    FROM generate_series(
        floor(extract(epoch from CAST(:chunk_start AS timestamp)) / :interval_seconds)::bigint * :interval_seconds,
        floor(extract(epoch from CAST(:chunk_end AS timestamp)) / :interval_seconds)::bigint * :interval_seconds - :interval_seconds,
        :interval_seconds
    ) gs
)
SELECT
    b.bin_start AS interval_begin_time,
    COUNT(DISTINCT sess.mac_address) AS number_connections
FROM bins b
JOIN user_ap_trajectory sess ON
    sess.space_id = :space_id
    AND sess.start_time < b.bin_start + make_interval(secs => :interval_seconds)
    AND sess.end_time   > b.bin_start
    AND sess.start_time < CAST(:chunk_end AS timestamp)
    AND sess.end_time   > CAST(:chunk_start AS timestamp)
GROUP BY b.bin_start
ORDER BY b.bin_start
"""


@op(
    required_resource_keys={"s3_storage"},
    config_schema={"chunk_id": int},
)
def materialize_source_chunk(context: OpExecutionContext) -> None:
    """Compute occupancy bins for one source space chunk and upload to S3."""
    chunk_id = context.op_config["chunk_id"]

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        row = session.execute(
            text("SELECT * FROM occupancy_space_chunks WHERE chunk_id = :id"),
            {"id": chunk_id}
        ).fetchone()

        if not row:
            raise ValueError(f"OccupancySpaceChunk {chunk_id} not found")

        space_id = row.space_id
        interval_seconds = row.interval_seconds
        chunk_start = row.chunk_start
        chunk_end = row.chunk_end

        session.execute(
            text("UPDATE occupancy_space_chunks SET status = 'RUNNING' WHERE chunk_id = :id"),
            {"id": chunk_id}
        )
        session.commit()

        context.log.info(f"Source chunk {chunk_id}: space={space_id}, {chunk_start} -> {chunk_end}")

        tippers_engine = get_tippers_engine()
        params = {
            "space_id": space_id,
            "chunk_start": chunk_start.isoformat(),
            "chunk_end": chunk_end.isoformat(),
            "interval_seconds": interval_seconds,
        }
        df = pd.read_sql(text(SOURCE_CHUNK_SQL), tippers_engine, params=params)

        context.log.info(f"Source chunk {chunk_id}: {len(df)} bins computed")

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        chunk_start_str = chunk_start.strftime('%Y%m%dT%H%M%S')
        chunk_end_str = chunk_end.strftime('%Y%m%dT%H%M%S')
        s3_key = f"occupancy/spaces/{space_id}/{interval_seconds}/chunk_{chunk_start_str}_{chunk_end_str}.parquet"

        local_path = f"/tmp/source_chunk_{chunk_id}.parquet"
        df.to_parquet(local_path, index=False)
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        storage_path = f"s3://{s3_bucket}/{s3_key}"

        context.log.info(f"Source chunk {chunk_id} uploaded to {storage_path}")

        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'COMPLETED', storage_path = :path, completed_at = :now
            WHERE chunk_id = :id
            """),
            {"path": storage_path, "now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()

    except Exception as e:
        context.log.error(f"Source chunk {chunk_id} failed: {e}")
        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'FAILED', error_message = :err, completed_at = :now
            WHERE chunk_id = :id
            """),
            {"err": str(e), "now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()
        raise
    finally:
        session.close()


@op(
    required_resource_keys={"s3_storage"},
    config_schema={"chunk_id": int},
)
def materialize_derived_chunk(context: OpExecutionContext) -> None:
    """Sum children's chunk parquets to produce one derived space chunk."""
    chunk_id = context.op_config["chunk_id"]

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        row = session.execute(
            text("SELECT * FROM occupancy_space_chunks WHERE chunk_id = :id"),
            {"id": chunk_id}
        ).fetchone()

        if not row:
            raise ValueError(f"OccupancySpaceChunk {chunk_id} not found")

        space_id = row.space_id
        interval_seconds = row.interval_seconds
        chunk_start = row.chunk_start
        chunk_end = row.chunk_end

        session.execute(
            text("UPDATE occupancy_space_chunks SET status = 'RUNNING' WHERE chunk_id = :id"),
            {"id": chunk_id}
        )
        session.commit()

        context.log.info(f"Derived chunk {chunk_id}: space={space_id}, {chunk_start} -> {chunk_end}")

        tippers_engine = get_tippers_engine()
        with tippers_engine.connect() as tconn:
            children = tconn.execute(
                text("SELECT space_id FROM space WHERE parent_space_id = :pid"),
                {"pid": space_id}
            ).fetchall()
        child_ids = [c.space_id for c in children]

        context.log.info(f"Derived chunk {chunk_id}: {len(child_ids)} children to aggregate")

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name

        dfs = []
        for child_id in child_ids:
            child_row = session.execute(
                text("""
                SELECT storage_path FROM occupancy_space_chunks
                WHERE space_id = :sid
                  AND interval_seconds = :interval
                  AND chunk_start = :cs
                  AND chunk_end = :ce
                  AND status = 'COMPLETED'
                """),
                {
                    "sid": child_id,
                    "interval": interval_seconds,
                    "cs": chunk_start,
                    "ce": chunk_end,
                }
            ).fetchone()

            if not child_row or not child_row.storage_path:
                context.log.info(f"Child space {child_id}: no COMPLETED chunk, skipping")
                continue

            storage_path = child_row.storage_path
            if storage_path.startswith("s3://"):
                parts = storage_path.replace("s3://", "").split("/", 1)
                local_path = f"/tmp/derived_child_{child_id}_chunk_{chunk_id}.parquet"
                s3_client.download_file(parts[0], parts[1], local_path)
                dfs.append(pd.read_parquet(local_path))
            elif os.path.exists(storage_path):
                dfs.append(pd.read_parquet(storage_path))

        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            df = (
                combined.groupby('interval_begin_time', as_index=False)['number_connections']
                .sum()
                .sort_values('interval_begin_time')
                .reset_index(drop=True)
            )
        else:
            df = pd.DataFrame(columns=['interval_begin_time', 'number_connections'])

        context.log.info(f"Derived chunk {chunk_id}: {len(df)} aggregated bins")

        chunk_start_str = chunk_start.strftime('%Y%m%dT%H%M%S')
        chunk_end_str = chunk_end.strftime('%Y%m%dT%H%M%S')
        s3_key = f"occupancy/spaces/{space_id}/{interval_seconds}/chunk_{chunk_start_str}_{chunk_end_str}.parquet"

        local_path = f"/tmp/derived_chunk_{chunk_id}.parquet"
        df.to_parquet(local_path, index=False)
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        storage_path = f"s3://{s3_bucket}/{s3_key}"

        context.log.info(f"Derived chunk {chunk_id} uploaded to {storage_path}")

        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'COMPLETED', storage_path = :path, completed_at = :now
            WHERE chunk_id = :id
            """),
            {"path": storage_path, "now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()

    except Exception as e:
        context.log.error(f"Derived chunk {chunk_id} failed: {e}")
        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'FAILED', error_message = :err, completed_at = :now
            WHERE chunk_id = :id
            """),
            {"err": str(e), "now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()
        raise
    finally:
        session.close()


@graph
def source_chunk_graph():
    materialize_source_chunk()


@graph
def derived_chunk_graph():
    materialize_derived_chunk()


@sensor(minimum_interval_seconds=30, name="occupancy_space_chunk_sensor", job_name="materialize_derived_chunk_job")
def occupancy_space_chunk_sensor(context: SensorEvaluationContext):
    """
    Triggers derived chunk jobs when all their children's source chunks are COMPLETED.
    Uses run_key per chunk_id so Dagster deduplicates submissions automatically.
    Processes chunks in batches to avoid timeout.
    Includes memory-based backpressure.
    """
    from backend.utils.backpressure import should_submit_jobs, get_memory_usage_percent
    from backend.utils.timeout_calculator import get_timeout_calculator

    # Backpressure: skip submission if memory > 80%
    if not should_submit_jobs(threshold_percent=80.0):
        usage = get_memory_usage_percent()
        context.log.warning(f"Backpressure active: memory at {usage:.1f}%, skipping derived chunk submission")
        return

    BATCH_SIZE = 50  # Process max 50 chunks per sensor tick to avoid timeout

    engine = get_db_engine()
    tippers_engine = get_tippers_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    timeout_calc = get_timeout_calculator()

    # Use cursor to track last processed chunk_id
    cursor = context.cursor or "0"
    last_chunk_id = int(cursor)

    try:
        pending_derived = session.execute(
            text("""
            SELECT chunk_id, space_id, interval_seconds, chunk_start, chunk_end, retry_count
            FROM occupancy_space_chunks
            WHERE space_type = 'derived' AND status = 'PENDING' AND chunk_id > :last_id
            ORDER BY chunk_id
            LIMIT :batch_size
            """),
            {"last_id": last_chunk_id, "batch_size": BATCH_SIZE}
        ).fetchall()

        for chunk in pending_derived:
            try:
                with tippers_engine.connect() as tconn:
                    children = tconn.execute(
                        text("SELECT space_id FROM space WHERE parent_space_id = :pid"),
                        {"pid": chunk.space_id}
                    ).fetchall()
                child_ids = [c.space_id for c in children]
            except Exception as e:
                context.log.warning(f"Tippers query failed for space {chunk.space_id}: {e}")
                continue

            if not child_ids:
                continue

            # All children WITH chunk records for this window must be COMPLETED.
            # Children with no record have no data and contribute 0 (skip gracefully).
            child_records = session.execute(
                text("""
                SELECT space_id, status FROM occupancy_space_chunks
                WHERE space_id = ANY(:child_ids)
                  AND interval_seconds = :interval
                  AND chunk_start = :cs
                  AND chunk_end = :ce
                """),
                {
                    "child_ids": child_ids,
                    "interval": chunk.interval_seconds,
                    "cs": chunk.chunk_start,
                    "ce": chunk.chunk_end,
                }
            ).fetchall()

            all_ready = all(r.status == 'COMPLETED' for r in child_records)
            if not all_ready:
                continue

            # Calculate timeout for derived chunk before yielding RunRequest
            timeout_seconds = timeout_calc.calculate_timeout(
                session=session,
                interval_seconds=chunk.interval_seconds,
                space_type='derived'
            )

            # Store timeout before job submission
            session.execute(
                text("""
                    UPDATE occupancy_space_chunks
                    SET timeout_seconds = :timeout
                    WHERE chunk_id = :chunk_id
                """),
                {"timeout": timeout_seconds, "chunk_id": chunk.chunk_id}
            )
            session.commit()

            # Include retry_count in run_key so retries get submitted
            run_key = f"derived_chunk_{chunk.chunk_id}_r{chunk.retry_count}"

            yield RunRequest(
                run_key=run_key,
                job_name="materialize_derived_chunk_job",
                run_config={
                    "ops": {
                        "materialize_derived_chunk": {
                            "config": {"chunk_id": chunk.chunk_id}
                        }
                    }
                },
                tags={
                    "chunk_id": str(chunk.chunk_id),
                    "space_id": str(chunk.space_id),
                    "chunk_type": "derived",
                    "interval_seconds": str(chunk.interval_seconds),
                },
            )

        # Update cursor: if we processed a full batch, continue from last chunk_id
        # If batch was smaller, reset to 0 to scan from beginning next tick
        if pending_derived:
            max_chunk_id = max(c.chunk_id for c in pending_derived)
            if len(pending_derived) == BATCH_SIZE:
                context.update_cursor(str(max_chunk_id))
            else:
                # Processed all remaining, reset cursor to start fresh next tick
                context.update_cursor("0")

    except Exception as e:
        context.log.error(f"occupancy_space_chunk_sensor error: {e}")
    finally:
        session.close()


@sensor(
    minimum_interval_seconds=int(os.getenv("TIMEOUT_SENSOR_INTERVAL_SECONDS", "60")),
    name="occupancy_chunk_timeout_sensor"
)
def occupancy_chunk_timeout_sensor(context: SensorEvaluationContext):
    """
    Monitor RUNNING chunks for timeouts and handle retries.

    For each timed-out chunk:
    - First timeout (retry_count=0): Terminate run, reset to PENDING, increment retry_count
      (source_chunk_submission_sensor will pick it up for retry)
    - Second timeout (retry_count=1): Terminate run, mark FAILED with timeout error

    Uses context.instance for run termination (no webserver dependency).
    """
    from dagster import DagsterRunStatus, RunsFilter

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Find RUNNING chunks where NOW() - created_at > timeout_seconds
        running_chunks = session.execute(
            text("""
                SELECT
                    chunk_id, space_id, interval_seconds, chunk_start, chunk_end,
                    space_type, retry_count, timeout_seconds,
                    created_at,
                    EXTRACT(EPOCH FROM (NOW() - created_at)) as elapsed_seconds
                FROM occupancy_space_chunks
                WHERE status = 'RUNNING'
                  AND timeout_seconds IS NOT NULL
                  AND EXTRACT(EPOCH FROM (NOW() - created_at)) > timeout_seconds
            """)
        ).fetchall()

        for chunk in running_chunks:
            chunk_id = chunk.chunk_id
            retry_count = chunk.retry_count
            space_type = chunk.space_type

            context.log.warning(
                f"Chunk {chunk_id} ({space_type}) timed out after {chunk.elapsed_seconds:.0f}s "
                f"(limit: {chunk.timeout_seconds}s, retry_count: {retry_count})"
            )

            # Try to terminate any running Dagster runs for this chunk using tag lookup
            try:
                runs = context.instance.get_runs(
                    filters=RunsFilter(
                        tags={"chunk_id": str(chunk_id)},
                        statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.STARTING]
                    ),
                    limit=1
                )
                if runs:
                    run_id = runs[0].run_id
                    context.instance.run_launcher.terminate(run_id)
                    context.log.info(f"Terminated run {run_id} for chunk {chunk_id}")
            except Exception as e:
                context.log.warning(f"Failed to terminate run for chunk {chunk_id}: {e}")

            if retry_count == 0:
                # First timeout - reset to PENDING for retry by submission sensor
                context.log.info(f"Resetting chunk {chunk_id} to PENDING for retry")

                session.execute(
                    text("""
                        UPDATE occupancy_space_chunks
                        SET status = 'PENDING',
                            error_message = NULL,
                            completed_at = NULL,
                            retry_count = :new_retry_count
                        WHERE chunk_id = :chunk_id
                    """),
                    {"chunk_id": chunk_id, "new_retry_count": retry_count + 1}
                )
                session.commit()
                # Submission sensor will pick this up with new run_key suffix

            else:
                # retry_count >= 1 - second timeout, mark FAILED
                context.log.error(
                    f"Chunk {chunk_id} exceeded timeout after retry, marking FAILED"
                )

                session.execute(
                    text("""
                        UPDATE occupancy_space_chunks
                        SET status = 'FAILED',
                            error_message = :error,
                            completed_at = NOW()
                        WHERE chunk_id = :chunk_id
                    """),
                    {
                        "chunk_id": chunk_id,
                        "error": f"Exceeded timeout ({chunk.timeout_seconds}s) after retry. Elapsed: {chunk.elapsed_seconds:.0f}s"
                    }
                )
                session.commit()

    except Exception as e:
        context.log.error(f"occupancy_chunk_timeout_sensor error: {e}")
    finally:
        session.close()


@sensor(
    minimum_interval_seconds=int(os.getenv("SOURCE_CHUNK_SENSOR_INTERVAL", "30")),
    name="source_chunk_submission_sensor",
    job_name="materialize_source_chunk_job"
)
def source_chunk_submission_sensor(context: SensorEvaluationContext):
    """
    Submits Dagster jobs for PENDING source chunks.
    Uses RunRequest for webserver-independent submission.
    Includes memory-based backpressure and tag-based concurrency control.
    """
    from backend.utils.backpressure import should_submit_jobs, get_memory_usage_percent
    from backend.utils.timeout_calculator import get_timeout_calculator

    # Backpressure: skip submission if memory > 80%
    if not should_submit_jobs(threshold_percent=80.0):
        usage = get_memory_usage_percent()
        context.log.warning(f"Backpressure active: memory at {usage:.1f}%, skipping submission")
        return

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    timeout_calc = get_timeout_calculator()

    BATCH_SIZE = 100

    try:
        # Find PENDING source chunks (use run_key for deduplication, not dagster_run_id)
        pending_chunks = session.execute(
            text("""
                SELECT chunk_id, space_id, interval_seconds, retry_count
                FROM occupancy_space_chunks
                WHERE space_type = 'source'
                  AND status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT :batch_size
            """),
            {"batch_size": BATCH_SIZE}
        ).fetchall()

        if not pending_chunks:
            return  # No chunks to submit

        context.log.info(f"Found {len(pending_chunks)} pending source chunks to submit")

        # Group chunks by interval_seconds to calculate timeout once per interval
        chunks_by_interval = {}
        for chunk in pending_chunks:
            interval = chunk.interval_seconds
            if interval not in chunks_by_interval:
                chunks_by_interval[interval] = []
            chunks_by_interval[interval].append((chunk.chunk_id, chunk.space_id, chunk.retry_count))

        for interval_seconds, chunk_data in chunks_by_interval.items():
            # Calculate timeout for this interval
            timeout_seconds = timeout_calc.calculate_timeout(
                session=session,
                interval_seconds=interval_seconds,
                space_type='source'
            )

            for chunk_id, space_id, retry_count in chunk_data:
                # Store timeout before yielding (we won't have run_id)
                session.execute(
                    text("""
                        UPDATE occupancy_space_chunks
                        SET timeout_seconds = :timeout
                        WHERE chunk_id = :chunk_id
                    """),
                    {"timeout": timeout_seconds, "chunk_id": chunk_id}
                )

                run_config = {
                    "ops": {
                        "materialize_source_chunk": {
                            "config": {"chunk_id": chunk_id}
                        }
                    }
                }

                # Include retry_count in run_key so retries get submitted
                run_key = f"source_chunk_{chunk_id}_r{retry_count}"

                # Yield RunRequest - Dagster handles submission without webserver
                yield RunRequest(
                    run_key=run_key,
                    job_name="materialize_source_chunk_job",
                    run_config=run_config,
                    tags={
                        "chunk_id": str(chunk_id),
                        "space_id": str(space_id),
                        "chunk_type": "source",
                        "interval_seconds": str(interval_seconds),
                    },
                )

        session.commit()
        context.log.info(f"Yielded {len(pending_chunks)} source chunk RunRequests")

    except Exception as e:
        context.log.error(f"source_chunk_submission_sensor error: {e}")
    finally:
        session.close()


@sensor(
    minimum_interval_seconds=10,
    name="dataset_initialization_sensor"
)
def dataset_initialization_sensor(context: SensorEvaluationContext):
    """
    Background sensor that initializes datasets by creating their chunk records.
    Runs every 10 seconds, finds datasets with status='INITIALIZING', creates chunks, marks as 'RUNNING'.
    """
    engine = get_db_engine()
    tippers_engine = get_tippers_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Find datasets waiting to be initialized
        datasets = session.execute(
            text("SELECT * FROM occupancy_datasets WHERE status = 'INITIALIZING'")
        ).fetchall()

        for dataset in datasets:
            try:
                context.log.info(f"Initializing dataset {dataset.dataset_id}: {dataset.name}")

                # Query space hierarchy
                with tippers_engine.connect() as tconn:
                    subtree_rows = tconn.execute(text("""
                        WITH RECURSIVE subtree AS (
                            SELECT space_id, parent_space_id FROM space WHERE space_id = :root
                            UNION ALL
                            SELECT s.space_id, s.parent_space_id FROM space s
                            JOIN subtree st ON s.parent_space_id = st.space_id
                        )
                        SELECT space_id, parent_space_id FROM subtree
                    """), {"root": dataset.root_space_id}).fetchall()

                all_space_ids = [r.space_id for r in subtree_rows]
                parent_map = {r.space_id: r.parent_space_id for r in subtree_rows}

                # Query date range per space (min/max dates with data)
                with tippers_engine.connect() as tconn:
                    source_date_ranges = tconn.execute(text("""
                        SELECT
                            space_id,
                            MIN(date_trunc('day', start_time))::date as min_date,
                            (MAX(date_trunc('day', end_time))::date + 1) as max_date
                        FROM user_ap_trajectory
                        WHERE space_id = ANY(:space_ids)
                          AND start_time < :end_time
                          AND end_time > :start_time
                        GROUP BY space_id
                    """), {
                        "space_ids": all_space_ids,
                        "start_time": dataset.start_time,
                        "end_time": dataset.end_time,
                    }).fetchall()

                # Build map: {space_id: (min_date, max_date)}
                source_space_ranges = {}
                for row in source_date_ranges:
                    source_space_ranges[row.space_id] = (row.min_date, row.max_date)

                source_spaces = set(source_space_ranges.keys())

                if not source_spaces:
                    # No data - mark as COMPLETED
                    session.execute(
                        text("UPDATE occupancy_datasets SET status = 'COMPLETED', row_count = 0 WHERE dataset_id = :id"),
                        {"id": dataset.dataset_id}
                    )
                    session.commit()
                    continue

                # Find derived spaces
                derived_spaces = set()
                for space_id in source_spaces:
                    current = space_id
                    while True:
                        parent = parent_map.get(current)
                        if parent is None or parent not in set(all_space_ids):
                            break
                        derived_spaces.add(parent)
                        current = parent

                # Create source chunks (1 per space covering full date range)
                source_chunk_values = []
                for space_id, (min_date, max_date) in source_space_ranges.items():
                    chunk_start = datetime.combine(min_date, datetime.min.time())
                    chunk_end = datetime.combine(max_date, datetime.min.time())
                    source_chunk_values.append({
                        "space_id": space_id,
                        "interval": dataset.interval_seconds,
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                    })

                if source_chunk_values:
                    session.execute(text("""
                        INSERT INTO occupancy_space_chunks
                            (space_id, interval_seconds, chunk_start, chunk_end, space_type, status)
                        SELECT
                            unnest(CAST(:space_ids AS int[])),
                            unnest(CAST(:intervals AS int[])),
                            unnest(CAST(:chunk_starts AS timestamp[])),
                            unnest(CAST(:chunk_ends AS timestamp[])),
                            'source',
                            'PENDING'
                        ON CONFLICT (space_id, interval_seconds, chunk_start, chunk_end) DO NOTHING
                    """), {
                        "space_ids": [v["space_id"] for v in source_chunk_values],
                        "intervals": [v["interval"] for v in source_chunk_values],
                        "chunk_starts": [v["chunk_start"] for v in source_chunk_values],
                        "chunk_ends": [v["chunk_end"] for v in source_chunk_values],
                    })

                # Create derived chunks (1 per space covering union of children's date ranges)
                depth_map = {}
                for space_id in set(all_space_ids):
                    depth = 0
                    current = space_id
                    while parent_map.get(current) is not None:
                        depth += 1
                        current = parent_map[current]
                    depth_map[space_id] = depth

                derived_spaces_sorted = sorted(derived_spaces, key=lambda s: depth_map[s], reverse=True)
                derived_space_ranges = {}
                derived_chunk_values = []

                for space_id in derived_spaces_sorted:
                    children = [sid for sid, parent in parent_map.items() if parent == space_id]
                    child_min_dates = []
                    child_max_dates = []
                    for child_id in children:
                        if child_id in source_space_ranges:
                            min_d, max_d = source_space_ranges[child_id]
                            child_min_dates.append(min_d)
                            child_max_dates.append(max_d)
                        elif child_id in derived_space_ranges:
                            min_d, max_d = derived_space_ranges[child_id]
                            child_min_dates.append(min_d)
                            child_max_dates.append(max_d)

                    if not child_min_dates:
                        continue

                    # Union of children's date ranges
                    min_date = min(child_min_dates)
                    max_date = max(child_max_dates)
                    derived_space_ranges[space_id] = (min_date, max_date)

                    chunk_start = datetime.combine(min_date, datetime.min.time())
                    chunk_end = datetime.combine(max_date, datetime.min.time())
                    derived_chunk_values.append({
                        "space_id": space_id,
                        "interval": dataset.interval_seconds,
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                    })

                if derived_chunk_values:
                    session.execute(text("""
                        INSERT INTO occupancy_space_chunks
                            (space_id, interval_seconds, chunk_start, chunk_end, space_type, status)
                        SELECT
                            unnest(CAST(:space_ids AS int[])),
                            unnest(CAST(:intervals AS int[])),
                            unnest(CAST(:chunk_starts AS timestamp[])),
                            unnest(CAST(:chunk_ends AS timestamp[])),
                            'derived',
                            'PENDING'
                        ON CONFLICT (space_id, interval_seconds, chunk_start, chunk_end) DO NOTHING
                    """), {
                        "space_ids": [v["space_id"] for v in derived_chunk_values],
                        "intervals": [v["interval"] for v in derived_chunk_values],
                        "chunk_starts": [v["chunk_start"] for v in derived_chunk_values],
                        "chunk_ends": [v["chunk_end"] for v in derived_chunk_values],
                    })

                # Mark dataset as RUNNING (chunks created, ready for job submission)
                session.execute(
                    text("UPDATE occupancy_datasets SET status = 'RUNNING' WHERE dataset_id = :id"),
                    {"id": dataset.dataset_id}
                )
                session.commit()
                context.log.info(f"Dataset {dataset.dataset_id} initialized - {len(source_chunk_values)} source + {len(derived_chunk_values)} derived chunks created")

            except Exception as e:
                context.log.error(f"Failed to initialize dataset {dataset.dataset_id}: {e}")
                session.rollback()
                # Mark as FAILED
                session.execute(
                    text("UPDATE occupancy_datasets SET status = 'FAILED', error_message = :err WHERE dataset_id = :id"),
                    {"err": str(e), "id": dataset.dataset_id}
                )
                session.commit()

    except Exception as e:
        context.log.error(f"dataset_initialization_sensor error: {e}")
    finally:
        session.close()
