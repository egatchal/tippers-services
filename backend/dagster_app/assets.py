"""Dagster assets for weak supervision pipeline."""
from dagster import (
    asset, AssetExecutionContext, Output,
    op, OpExecutionContext,
    graph, In, Out, DynamicOutput, DynamicOut,
    run_failure_sensor, RunFailureSensorContext,
    sensor, SensorEvaluationContext,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import os
from datetime import datetime
from typing import Dict, Any, List
import json
from cryptography.fernet import Fernet
from jinja2 import Template
from backend.utils.dataset_registry import register_dataset
from backend.utils.resolve_entities import resolve_entity_ids


def _update_unified_job(session, dagster_run_id: str, status: str, error_message: str = None):
    """Update the unified jobs table for this Dagster run. Non-critical — never raises."""
    try:
        now = datetime.utcnow()
        if status in ("COMPLETED", "FAILED"):
            session.execute(
                text("""
                    UPDATE jobs SET status = :status, error_message = :error, completed_at = :now
                    WHERE dagster_run_id = :run_id AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """),
                {"status": status, "error": error_message, "now": now, "run_id": dagster_run_id},
            )
        else:
            session.execute(
                text("""
                    UPDATE jobs SET status = :status, started_at = COALESCE(started_at, :now)
                    WHERE dagster_run_id = :run_id
                """),
                {"status": status, "now": now, "run_id": dagster_run_id},
            )
        session.commit()
    except Exception:
        pass  # Non-critical


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


def decrypt_password(encrypted_password: str | None) -> str:
    """Decrypt password using encryption key from environment."""
    if not encrypted_password:
        return ""
    encryption_key = os.getenv("ENCRYPTION_KEY")
    if not encryption_key:
        raise ValueError("ENCRYPTION_KEY environment variable not set")
    cipher_suite = Fernet(encryption_key.encode() if isinstance(encryption_key, str) else encryption_key)
    return cipher_suite.decrypt(encrypted_password.encode()).decode()


def build_conn_string(conn_type: str, user: str, password: str, host: str, port: int, database: str) -> str:
    """Build a SQLAlchemy connection string, omitting password if empty."""
    if password:
        return f"{conn_type}://{user}:{password}@{host}:{port}/{database}"
    return f"{conn_type}://{user}@{host}:{port}/{database}"


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
        password = decrypt_password(index_row.encrypted_password)
        external_conn_str = build_conn_string(
            index_row.connection_type, index_row.user, password,
            index_row.host, index_row.port, index_row.database,
        )

        context.log.info(f"Connecting to external database: {index_row.host}:{index_row.port}/{index_row.database}")

        # Execute query on external database
        external_engine = create_engine(external_conn_str)
        sql_query = index_row.sql_query

        context.log.info(f"Executing query: {sql_query[:100]}...")

        df = pd.read_sql(sql_query, external_engine)

        context.log.info(f"Query returned {len(df)} rows")

        # Extract unique key values if key_column is set (index = key set)
        key_column = index_row.key_column
        if key_column:
            if key_column not in df.columns:
                raise ValueError(f"key_column '{key_column}' not found. Available: {list(df.columns)}")
            df = df[[key_column]].drop_duplicates().reset_index(drop=True)
            context.log.info(f"Extracted {len(df)} unique keys from column '{key_column}'")

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

        # Cascade: update derived indexes that depend on this SQL index
        try:
            derived_rows = session.execute(
                text("""
                SELECT index_id FROM concept_indexes
                WHERE parent_index_id = :index_id AND source_type = 'derived'
                """),
                {"index_id": index_id},
            ).fetchall()

            if derived_rows:
                from backend.utils.resolve_entities import resolve_entity_ids as _resolve_ids
                for d_row in derived_rows:
                    try:
                        df_derived = _resolve_ids(d_row.index_id, session)
                        derived_stats = compute_column_stats(df_derived)
                        session.execute(
                            text("""
                            UPDATE concept_indexes
                            SET row_count = :row_count,
                                filtered_count = :filtered_count,
                                column_stats = :column_stats,
                                materialized_at = :now,
                                updated_at = :now
                            WHERE index_id = :did
                            """),
                            {
                                "did": d_row.index_id,
                                "row_count": len(df_derived),
                                "filtered_count": len(df_derived),
                                "column_stats": json.dumps(derived_stats),
                                "now": datetime.utcnow(),
                            },
                        )
                        context.log.info(
                            f"Cascade-updated derived index {d_row.index_id}: {len(df_derived)} rows"
                        )
                    except Exception as de:
                        context.log.warning(f"Failed to cascade-update derived index {d_row.index_id}: {de}")
                session.commit()
        except Exception as cascade_err:
            context.log.warning(f"Failed to cascade-update derived indexes: {cascade_err}")

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Index {index_id}",
                service="snorkel",
                dataset_type="index",
                storage_path=storage_path,
                row_count=len(df),
                column_stats=column_stats,
                schema_info={"columns": {c: str(df[c].dtype) for c in df.columns}},
                source_ref={"entity_type": "concept_index", "entity_id": index_id},
                tags={"concept_id": int(index_row.c_id)},
            )
        except Exception as cat_err:
            context.log.warning(f"Failed to register dataset in catalog: {cat_err}")

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
        _update_unified_job(session, context.run_id, "FAILED", str(e))
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
                   ci.source_type as index_source_type, ci.key_column as index_key_column,
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

        # Resolve entity IDs — works for both SQL and derived indexes
        from backend.utils.resolve_entities import resolve_entity_ids
        df_index = resolve_entity_ids(rule_row.index_id, session)

        context.log.info(f"Resolved {len(df_index)} entity rows from index {rule_row.index_id}")

        # Extract values from index_column for filtering
        # Fallback: rule.index_column → parent index.key_column → first column
        index_column = rule_row.index_column or rule_row.index_key_column or df_index.columns[0]
        context.log.info(f"Extracting values from column: {index_column}")

        if index_column not in df_index.columns:
            raise ValueError(f"Column '{index_column}' not found in index data. Available columns: {list(df_index.columns)}")

        index_values = df_index[index_column].unique().tolist()
        context.log.info(f"Extracted {len(index_values)} unique values from {index_column}")

        # Connect to external database
        password = decrypt_password(rule_row.encrypted_password)
        external_conn_str = build_conn_string(
            rule_row.connection_type, rule_row.user, password,
            rule_row.host, rule_row.port, rule_row.database,
        )
        context.log.info(f"Connecting to external database: {rule_row.host}:{rule_row.port}/{rule_row.database}")
        external_engine = create_engine(external_conn_str)

        # Use temp table approach for entity IDs — avoids SQL string length issues
        # and SQL injection; works for any entity count
        with external_engine.connect() as ext_conn:
            # Determine PG type from first value
            sample_val = index_values[0] if index_values else ""
            pg_type = "TEXT" if isinstance(sample_val, str) else "BIGINT"

            ext_conn.execute(text(f"CREATE TEMP TABLE _tippers_entity_ids (entity_id {pg_type}) ON COMMIT DROP"))

            # Batch insert entity IDs in chunks of 1000
            BATCH_SIZE = 1000
            for i in range(0, len(index_values), BATCH_SIZE):
                batch = index_values[i:i + BATCH_SIZE]
                values_clause = ", ".join([f"(:v{j})" for j in range(len(batch))])
                params = {f"v{j}": v for j, v in enumerate(batch)}
                ext_conn.execute(text(f"INSERT INTO _tippers_entity_ids (entity_id) VALUES {values_clause}"), params)

            context.log.info(f"Inserted {len(index_values)} entity IDs into temp table")

            # Replace :index_values placeholder with temp table subquery
            rendered_sql = rule_row.sql_query.replace(
                ":index_values",
                "(SELECT entity_id FROM _tippers_entity_ids)"
            )

            # Support Jinja2 template params
            if rule_row.query_template_params:
                sql_template = Template(rendered_sql)
                rendered_sql = sql_template.render(**rule_row.query_template_params)

            context.log.info(f"Rendered SQL query (first 200 chars): {rendered_sql[:200]}...")

            df_features = pd.read_sql(text(rendered_sql), ext_conn)

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

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Rule {rule_id} features",
                service="snorkel",
                dataset_type="rule_features",
                storage_path=storage_path,
                row_count=len(df_features),
                column_stats=column_stats,
                schema_info={"columns": {c: str(df_features[c].dtype) for c in df_features.columns}},
                source_ref={"entity_type": "concept_rule", "entity_id": rule_id},
                tags={"concept_id": int(rule_row.c_id)},
            )
        except Exception as cat_err:
            context.log.warning(f"Failed to register dataset in catalog: {cat_err}")

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
        _update_unified_job(session, context.run_id, "FAILED", str(e))
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
                   ci.key_column as index_key_column,
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
        # Fallback: feature.index_column → parent index.key_column → first column
        index_column = feature_row.index_column or feature_row.index_key_column or df_index.columns[0]
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
        external_conn_str = build_conn_string(
            feature_row.connection_type, feature_row.user, password,
            feature_row.host, feature_row.port, feature_row.database,
        )

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

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Feature {feature_id}",
                service="snorkel",
                dataset_type="features",
                storage_path=storage_path,
                row_count=len(df_features),
                column_stats=column_stats,
                schema_info={"columns": {c: str(df_features[c].dtype) for c in df_features.columns}},
                source_ref={"entity_type": "concept_feature", "entity_id": feature_id},
                tags={"concept_id": int(feature_row.c_id)},
            )
        except Exception as cat_err:
            context.log.warning(f"Failed to register dataset in catalog: {cat_err}")

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
        _update_unified_job(session, context.run_id, "FAILED", str(e))
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Occupancy — incremental bottom-up materialization
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
    config_schema={"dataset_id": int},
    out=DynamicOut(),
)
def plan_source_chunks(context: OpExecutionContext):
    """
    Load the dataset record, find all PENDING source (leaf) chunks in its subtree,
    and yield one DynamicOutput per chunk for parallel fan-out.
    """
    dataset_id = context.op_config["dataset_id"]

    engine = get_db_engine()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        ds = session.execute(
            text("SELECT * FROM occupancy_datasets WHERE dataset_id = :id"),
            {"id": dataset_id}
        ).fetchone()

        if not ds:
            raise ValueError(f"Occupancy dataset {dataset_id} not found")

        root_space_id = ds.root_space_id
        interval_seconds = ds.interval_seconds

        # Mark dataset as RUNNING
        session.execute(
            text("UPDATE occupancy_datasets SET status = 'RUNNING' WHERE dataset_id = :id"),
            {"id": dataset_id}
        )
        session.commit()

        # Build subtree from tippers
        tippers_engine = get_tippers_engine()
        with tippers_engine.connect() as tconn:
            subtree_rows = tconn.execute(
                text("""
                WITH RECURSIVE subtree AS (
                    SELECT space_id, parent_space_id FROM space WHERE space_id = :root
                    UNION ALL
                    SELECT s.space_id, s.parent_space_id FROM space s
                    JOIN subtree st ON s.parent_space_id = st.space_id
                )
                SELECT space_id, parent_space_id FROM subtree
                """),
                {"root": root_space_id}
            ).fetchall()
        subtree_ids = {r.space_id for r in subtree_rows}

        # Find PENDING source chunks in this subtree
        pending_chunks = session.execute(
            text("""
            SELECT chunk_id, space_id, chunk_start, chunk_end, space_type
            FROM occupancy_space_chunks
            WHERE interval_seconds = :interval
              AND status = 'PENDING'
              AND space_type = 'source'
            ORDER BY chunk_start
            """),
            {"interval": interval_seconds}
        ).fetchall()

        source_chunks = [c for c in pending_chunks if c.space_id in subtree_ids]

        context.log.info(
            f"Dataset {dataset_id}: {len(source_chunks)} PENDING source chunks to fan out"
        )

        for chunk in source_chunks:
            mapping_key = f"chunk_{chunk.chunk_id}"
            yield DynamicOutput(
                value={
                    "chunk_id": chunk.chunk_id,
                    "space_id": chunk.space_id,
                    "chunk_start": chunk.chunk_start.isoformat(),
                    "chunk_end": chunk.chunk_end.isoformat(),
                    "interval_seconds": interval_seconds,
                    "dataset_id": dataset_id,
                },
                mapping_key=mapping_key,
            )

    finally:
        session.close()


@op(
    required_resource_keys={"s3_storage"},
)
def process_source_chunk(context: OpExecutionContext, chunk_info: dict) -> dict:
    """
    Process a single source (leaf) chunk: query tippers, upload parquet to S3,
    mark COMPLETED. Runs in parallel via multiprocess executor.
    """
    chunk_id = chunk_info["chunk_id"]
    space_id = chunk_info["space_id"]
    chunk_start = chunk_info["chunk_start"]
    chunk_end = chunk_info["chunk_end"]
    interval_seconds = chunk_info["interval_seconds"]

    engine = get_db_engine()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        # Mark chunk RUNNING
        session.execute(
            text("UPDATE occupancy_space_chunks SET status = 'RUNNING' WHERE chunk_id = :id"),
            {"id": chunk_id}
        )
        session.commit()

        context.log.info(
            f"Source chunk {chunk_id}: space={space_id}, {chunk_start} -> {chunk_end}"
        )

        tippers_engine = get_tippers_engine()
        params = {
            "space_id": space_id,
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "interval_seconds": interval_seconds,
        }
        df = pd.read_sql(text(SOURCE_CHUNK_SQL), tippers_engine, params=params)

        cs_dt = datetime.fromisoformat(chunk_start)
        ce_dt = datetime.fromisoformat(chunk_end)
        cs_str = cs_dt.strftime('%Y%m%dT%H%M%S')
        ce_str = ce_dt.strftime('%Y%m%dT%H%M%S')
        s3_key = f"occupancy/spaces/{space_id}/{interval_seconds}/{cs_str}_{ce_str}.parquet"
        local_path = f"/tmp/source_chunk_{chunk_id}.parquet"
        df.to_parquet(local_path, index=False)

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        storage_path = f"s3://{s3_bucket}/{s3_key}"

        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'COMPLETED', storage_path = :path, completed_at = :now
            WHERE chunk_id = :id
            """),
            {"path": storage_path, "now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()
        context.log.info(f"Source chunk {chunk_id}: {len(df)} bins -> {storage_path}")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Occupancy space {space_id} ({interval_seconds}s)",
                service="occupancy",
                dataset_type="occupancy_space_chunk",
                storage_path=storage_path,
                row_count=len(df),
                source_ref={"entity_type": "occupancy_space_chunk", "space_id": space_id, "interval_seconds": interval_seconds},
                tags={"space_id": space_id, "interval_seconds": interval_seconds, "space_type": "source"},
            )
        except Exception:
            pass  # Non-critical

        return chunk_info

    except Exception as e:
        session.execute(
            text("""
            UPDATE occupancy_space_chunks
            SET status = 'FAILED', completed_at = :now
            WHERE chunk_id = :id AND status != 'COMPLETED'
            """),
            {"now": datetime.utcnow(), "id": chunk_id}
        )
        session.commit()
        raise
    finally:
        session.close()


@op(
    required_resource_keys={"s3_storage"},
    config_schema={"dataset_id": int},
)
def aggregate_and_assemble(context: OpExecutionContext, source_results: list) -> None:
    """
    Phase 1: Process derived (parent) chunks bottom-up — sum children's COMPLETED parquets.
    Phase 2: Assemble final dataset parquet from root space's COMPLETED chunks.

    Runs after all source chunks complete (guaranteed by .collect()).
    """
    dataset_id = context.op_config["dataset_id"]

    engine = get_db_engine()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        ds = session.execute(
            text("SELECT * FROM occupancy_datasets WHERE dataset_id = :id"),
            {"id": dataset_id}
        ).fetchone()

        if not ds:
            raise ValueError(f"Occupancy dataset {dataset_id} not found")

        root_space_id = ds.root_space_id
        interval_seconds = ds.interval_seconds
        start_time = ds.start_time
        end_time = ds.end_time

        # Build subtree
        tippers_engine = get_tippers_engine()
        with tippers_engine.connect() as tconn:
            subtree_rows = tconn.execute(
                text("""
                WITH RECURSIVE subtree AS (
                    SELECT space_id, parent_space_id FROM space WHERE space_id = :root
                    UNION ALL
                    SELECT s.space_id, s.parent_space_id FROM space s
                    JOIN subtree st ON s.parent_space_id = st.space_id
                )
                SELECT space_id, parent_space_id FROM subtree
                """),
                {"root": root_space_id}
            ).fetchall()
        subtree_ids = {r.space_id for r in subtree_rows}
        parent_map = {r.space_id: r.parent_space_id for r in subtree_rows}

        # Find PENDING derived chunks in this subtree
        derived_chunks = session.execute(
            text("""
            SELECT chunk_id, space_id, chunk_start, chunk_end, space_type
            FROM occupancy_space_chunks
            WHERE interval_seconds = :interval
              AND status = 'PENDING'
              AND space_type = 'derived'
            ORDER BY chunk_start
            """),
            {"interval": interval_seconds}
        ).fetchall()
        derived_chunks = [c for c in derived_chunks if c.space_id in subtree_ids]

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name

        context.log.info(
            f"Dataset {dataset_id}: {len(source_results)} source chunks done, "
            f"{len(derived_chunks)} derived chunks to process"
        )

        # ----- Phase 1: Derived spaces (bottom-up by depth) -----
        def _depth(sid):
            d = 0
            cur = sid
            while parent_map.get(cur) is not None and parent_map[cur] in subtree_ids:
                cur = parent_map[cur]
                d += 1
            return d

        derived_chunks_sorted = sorted(derived_chunks, key=lambda c: -_depth(c.space_id))

        for chunk in derived_chunks_sorted:
            context.log.info(
                f"Derived chunk {chunk.chunk_id}: space={chunk.space_id}, "
                f"{chunk.chunk_start} -> {chunk.chunk_end}"
            )
            session.execute(
                text("UPDATE occupancy_space_chunks SET status = 'RUNNING' WHERE chunk_id = :id"),
                {"id": chunk.chunk_id}
            )
            session.commit()

            with tippers_engine.connect() as tconn:
                children = tconn.execute(
                    text("SELECT space_id FROM space WHERE parent_space_id = :pid"),
                    {"pid": chunk.space_id}
                ).fetchall()
            child_ids = [c.space_id for c in children]

            dfs = []
            for child_id in child_ids:
                child_row = session.execute(
                    text("""
                    SELECT storage_path FROM occupancy_space_chunks
                    WHERE space_id = :sid
                      AND interval_seconds = :interval
                      AND chunk_start = :cs AND chunk_end = :ce
                      AND status = 'COMPLETED'
                    """),
                    {
                        "sid": child_id,
                        "interval": interval_seconds,
                        "cs": chunk.chunk_start,
                        "ce": chunk.chunk_end,
                    }
                ).fetchone()
                if not child_row or not child_row.storage_path:
                    continue
                sp = child_row.storage_path
                if sp.startswith("s3://"):
                    parts = sp.replace("s3://", "").split("/", 1)
                    lp = f"/tmp/derived_child_{child_id}_chunk_{chunk.chunk_id}.parquet"
                    s3_client.download_file(parts[0], parts[1], lp)
                    dfs.append(pd.read_parquet(lp))
                elif os.path.exists(sp):
                    dfs.append(pd.read_parquet(sp))

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

            cs_str = chunk.chunk_start.strftime('%Y%m%dT%H%M%S')
            ce_str = chunk.chunk_end.strftime('%Y%m%dT%H%M%S')
            s3_key = f"occupancy/spaces/{chunk.space_id}/{interval_seconds}/{cs_str}_{ce_str}.parquet"
            local_path = f"/tmp/derived_chunk_{chunk.chunk_id}.parquet"
            df.to_parquet(local_path, index=False)
            s3_client.upload_file(local_path, s3_bucket, s3_key)
            storage_path = f"s3://{s3_bucket}/{s3_key}"

            session.execute(
                text("""
                UPDATE occupancy_space_chunks
                SET status = 'COMPLETED', storage_path = :path, completed_at = :now
                WHERE chunk_id = :id
                """),
                {"path": storage_path, "now": datetime.utcnow(), "id": chunk.chunk_id}
            )
            session.commit()
            context.log.info(f"Derived chunk {chunk.chunk_id}: {len(df)} aggregated bins -> {storage_path}")

        # ----- Phase 2: Assemble final dataset parquet -----
        root_chunks = session.execute(
            text("""
            SELECT storage_path, chunk_start FROM occupancy_space_chunks
            WHERE space_id = :sid
              AND interval_seconds = :interval
              AND status = 'COMPLETED'
              AND chunk_start < :end_time
              AND chunk_end > :start_time
            ORDER BY chunk_start
            """),
            {
                "sid": root_space_id,
                "interval": interval_seconds,
                "start_time": start_time,
                "end_time": end_time,
            }
        ).fetchall()

        dfs = []
        for rc in root_chunks:
            if not rc.storage_path:
                continue
            sp = rc.storage_path
            if sp.startswith("s3://"):
                parts = sp.replace("s3://", "").split("/", 1)
                lp = f"/tmp/root_assemble_{dataset_id}_{len(dfs)}.parquet"
                s3_client.download_file(parts[0], parts[1], lp)
                dfs.append(pd.read_parquet(lp))
            elif os.path.exists(sp):
                dfs.append(pd.read_parquet(sp))

        if dfs:
            df = pd.concat(dfs, ignore_index=True).sort_values('interval_begin_time').reset_index(drop=True)
        else:
            df = pd.DataFrame(columns=['interval_begin_time', 'number_connections'])

        column_stats = compute_column_stats(df) if len(df) > 0 else {}

        final_s3_key = f"occupancy/datasets/{dataset_id}/data.parquet"
        final_local_path = f"/tmp/occupancy_{dataset_id}_final.parquet"
        df.to_parquet(final_local_path, index=False)
        s3_client.upload_file(final_local_path, s3_bucket, final_s3_key)
        final_storage_path = f"s3://{s3_bucket}/{final_s3_key}"

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
                "path": final_storage_path,
                "rc": len(df),
                "cs": json.dumps(column_stats),
                "id": dataset_id,
            }
        )
        session.commit()

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Sync the linked parent datasets row (created at request time)
        try:
            session.execute(
                text("""
                UPDATE datasets
                SET storage_path = :path,
                    row_count = :rc,
                    column_stats = CAST(:cs AS jsonb),
                    schema_info = CAST(:si AS jsonb),
                    status = 'AVAILABLE',
                    updated_at = NOW()
                WHERE dataset_id = (
                    SELECT parent_dataset_id FROM occupancy_datasets WHERE dataset_id = :occ_id
                )
                """),
                {
                    "path": final_storage_path,
                    "rc": len(df),
                    "cs": json.dumps(column_stats),
                    "si": json.dumps({"columns": {c: str(df[c].dtype) for c in df.columns}}),
                    "occ_id": dataset_id,
                }
            )
            session.commit()
            context.log.info(f"Synced parent datasets row for occupancy dataset {dataset_id}")
        except Exception as cat_err:
            context.log.error(f"Failed to sync parent datasets row: {cat_err}")
            import traceback
            context.log.error(traceback.format_exc())

        context.log.info(
            f"Dataset {dataset_id} COMPLETED: {len(df)} rows -> {final_storage_path}"
        )

    except Exception as e:
        context.log.error(f"Dataset {dataset_id} FAILED: {e}")
        session.execute(
            text("""
            UPDATE occupancy_datasets
            SET status = 'FAILED', error_message = :error, completed_at = now()
            WHERE dataset_id = :id AND status != 'COMPLETED'
            """),
            {"error": str(e), "id": dataset_id}
        )
        # Mark the linked parent datasets row as STALE
        session.execute(
            text("""
            UPDATE datasets
            SET status = 'STALE', updated_at = NOW()
            WHERE dataset_id = (
                SELECT parent_dataset_id FROM occupancy_datasets WHERE dataset_id = :id
            )
              AND status != 'STALE'
            """),
            {"id": dataset_id}
        )
        session.commit()
        _update_unified_job(session, context.run_id, "FAILED", str(e))
        raise
    finally:
        session.close()


@graph
def occupancy_incremental_graph():
    source_results = plan_source_chunks().map(process_source_chunk)
    aggregate_and_assemble(source_results.collect())


@run_failure_sensor
def occupancy_dataset_failure_sensor(context: RunFailureSensorContext):
    """Mark occupancy dataset as FAILED if the Dagster run fails."""
    if context.dagster_run.job_name != "occupancy_incremental_job":
        return

    try:
        dataset_id = context.dagster_run.run_config["ops"]["plan_source_chunks"]["config"]["dataset_id"]
    except (KeyError, TypeError):
        return

    engine = get_db_engine()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
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
        # Also mark the linked parent datasets row as STALE
        session.execute(
            text("""
            UPDATE datasets
            SET status = 'STALE', updated_at = NOW()
            WHERE dataset_id = (
                SELECT parent_dataset_id FROM occupancy_datasets WHERE dataset_id = :id
            )
              AND status != 'STALE'
            """),
            {"id": dataset_id}
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

    Each LF runs on its own rule's feature DataFrame (no cross-rule join).
    The label matrix is anchored to the full index entity set so that
    entities not covered by a rule receive ABSTAIN (-1) in that LF's column.

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

        # ── Load the full index entity set as the anchor ──
        df_index = resolve_entity_ids(job_row.index_id, session)

        # Determine the key column from the index
        index_meta = session.execute(
            text("SELECT key_column FROM concept_indexes WHERE index_id = :idx"),
            {"idx": job_row.index_id}
        ).fetchone()
        anchor_col = (index_meta.key_column if index_meta and index_meta.key_column
                      else df_index.columns[0])

        # The anchor: ordered list of all entity IDs in the index
        anchor_ids = df_index[anchor_col].drop_duplicates().reset_index(drop=True)
        n_samples = len(anchor_ids)
        anchor_id_to_pos = {eid: pos for pos, eid in enumerate(anchor_ids)}

        context.log.info(f"Index anchor: {n_samples} entities, key_column='{anchor_col}'")

        # ── Load rule feature DataFrames (deduplicated by rule_id) ──
        s3_resource = context.resources.s3_storage
        rule_dfs = {}        # rule_id → DataFrame
        rule_key_cols = {}   # rule_id → key column name
        for lf in lfs:
            if lf.rule_id not in rule_dfs:
                context.log.info(f"Loading features for rule_id={lf.rule_id}")
                df_rule = load_rule_features(lf.rule_id, session, s3_resource, context)
                rule_dfs[lf.rule_id] = df_rule

                # Look up the rule's key column
                rule_meta = session.execute(
                    text("SELECT index_column FROM concept_rules WHERE r_id = :rule_id"),
                    {"rule_id": lf.rule_id}
                ).fetchone()
                key_col = (rule_meta.index_column if rule_meta and rule_meta.index_column
                           else df_rule.columns[0])
                rule_key_cols[lf.rule_id] = key_col

        # ── Apply each LF on its own rule's DataFrame, then align to anchor ──
        n_lfs = len(lfs)
        L = np.full((n_samples, n_lfs), -1, dtype=int)  # default ABSTAIN

        for i, lf in enumerate(lfs):
            context.log.info(f"Applying LF {i+1}/{n_lfs}: {lf.name} (rule_id={lf.rule_id})")
            valid_cv_ids = set(int(cv) for cv in lf.applicable_cv_ids)

            df_rule = rule_dfs[lf.rule_id]
            key_col = rule_key_cols[lf.rule_id]

            try:
                # Run LF on the rule's own feature DataFrame (complete data, no NaN)
                lf_labels = apply_custom_lf(
                    df_rule, lf.lf_config, valid_cv_ids,
                    cv_id_to_index, cv_name_to_id, context
                )

                # Map labels back to the anchor positions
                rule_entity_ids = df_rule[key_col].values
                for j, eid in enumerate(rule_entity_ids):
                    pos = anchor_id_to_pos.get(eid)
                    if pos is not None:
                        L[pos, i] = lf_labels[j]

            except Exception as e:
                context.log.error(f"Error applying LF {lf.name}: {str(e)}")
                # Column stays all -1 (ABSTAIN)

        context.log.info(f"Label matrix created: {n_samples} samples x {n_lfs} LFs")

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

        # Build predictions DataFrame (parquet): sample_id + probs array
        sample_ids = result_data.get("sample_ids", list(range(n_samples)))
        if "probabilities" in result_data:
            probs_list = result_data["probabilities"]
        else:
            # Hard labels → one-hot encoding
            labels = result_data.get("labels", [])
            probs_list = []
            for lbl in labels:
                one_hot = [0.0] * cardinality
                if 0 <= lbl < cardinality:
                    one_hot[lbl] = 1.0
                probs_list.append(one_hot)

        df_predictions = pd.DataFrame({
            "sample_id": sample_ids,
            "probs": probs_list,
        })

        # Store predictions parquet locally then upload to S3
        local_storage_path = get_storage_path("snorkel_job", job_id)
        df_predictions.to_parquet(local_storage_path, index=False)
        context.log.info(f"Predictions parquet saved locally to {local_storage_path}")

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"snorkel_jobs/job_{job_id}.parquet"

        try:
            s3_client.upload_file(local_storage_path, s3_bucket, s3_key)
            s3_path = f"s3://{s3_bucket}/{s3_key}"
            context.log.info(f"Predictions uploaded to S3: {s3_path}")
            storage_path = s3_path
        except Exception as s3_error:
            context.log.warning(f"Failed to upload to S3: {s3_error}. Using local path.")
            storage_path = local_storage_path

        # Write metadata to JSONB columns on snorkel_jobs row + update status
        session.execute(
            text("""
            UPDATE snorkel_jobs
            SET status = 'COMPLETED',
                result_path = :storage_path,
                completed_at = :now,
                lf_summary = :lf_summary,
                class_distribution = :class_distribution,
                overall_stats = :overall_stats,
                cv_id_to_index = :cv_id_to_index,
                cv_id_to_name = :cv_id_to_name
            WHERE job_id = :job_id
            """),
            {
                "job_id": job_id,
                "storage_path": storage_path,
                "now": datetime.utcnow(),
                "lf_summary": json.dumps(result_data.get("lf_summary", [])),
                "class_distribution": json.dumps({
                    "label_matrix": result_data.get("label_matrix_class_distribution", {}),
                    "model": result_data.get("model_class_distribution", {}),
                }),
                "overall_stats": json.dumps(result_data.get("overall_stats", {})),
                "cv_id_to_index": json.dumps(result_data.get("cv_id_to_index", {})),
                "cv_id_to_name": json.dumps(result_data.get("cv_id_to_name", {})),
            }
        )

        session.commit()

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Snorkel labels (job {job_id})",
                service="snorkel",
                dataset_type="labels",
                format="json",
                storage_path=storage_path,
                row_count=n_samples,
                source_ref={"entity_type": "snorkel_job", "entity_id": job_id},
                tags={"concept_id": int(job_row.c_id)},
            )
        except Exception as cat_err:
            context.log.warning(f"Failed to register dataset in catalog: {cat_err}")

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
        _update_unified_job(session, context.run_id, "FAILED", str(e))

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

        _update_unified_job(session, context.run_id, "COMPLETED")

        # Register in unified dataset catalog
        try:
            register_dataset(
                session,
                name=f"Classifier results (job {job_id})",
                service="snorkel",
                dataset_type="training",
                format="json",
                storage_path=storage_path,
                row_count=n_samples_after,
                source_ref={"entity_type": "classifier_job", "entity_id": job_id},
                tags={"concept_id": int(job_row.c_id)},
            )
        except Exception as cat_err:
            context.log.warning(f"Failed to register dataset in catalog: {cat_err}")

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
        _update_unified_job(session, context.run_id, "FAILED", str(e))

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
@sensor(minimum_interval_seconds=30, name="workflow_advance_sensor")
def workflow_advance_sensor(context: SensorEvaluationContext):
    """Poll running workflow runs and advance them by submitting unblocked steps."""
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        from backend.utils.workflow_engine import WorkflowEngine

        running_runs = session.execute(
            text("SELECT run_id FROM workflow_runs WHERE status = 'RUNNING'")
        ).fetchall()

        if not running_runs:
            return

        wf_engine = WorkflowEngine(session)
        for row in running_runs:
            try:
                wf_engine.advance_run(row.run_id)
                context.log.info(f"Advanced workflow run {row.run_id}")
            except Exception as e:
                context.log.warning(f"Failed to advance workflow run {row.run_id}: {e}")

    except Exception as e:
        context.log.error(f"workflow_advance_sensor error: {e}")
    finally:
        session.close()


# ============================================================================
# Pipeline Graph — Index → Rules (parallel) → Snorkel Training
# ============================================================================

@op(
    config_schema={"index_id": int},
    required_resource_keys={"s3_storage"},
    out=Out(int),
)
def materialize_index_op(context: OpExecutionContext) -> int:
    """Op wrapper: materialize a SQL index and return its index_id."""
    index_id = context.op_config["index_id"]

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
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

        # Execute query on external database
        password = decrypt_password(index_row.encrypted_password)
        external_conn_str = build_conn_string(
            index_row.connection_type, index_row.user, password,
            index_row.host, index_row.port, index_row.database,
        )
        external_engine = create_engine(external_conn_str)
        df = pd.read_sql(index_row.sql_query, external_engine)

        context.log.info(f"Index {index_id}: {len(df)} rows")

        column_stats = compute_column_stats(df)
        local_path = get_storage_path("index", index_id)
        df.to_parquet(local_path, index=False)

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"indexes/index_{index_id}.parquet"

        try:
            s3_client.upload_file(local_path, s3_bucket, s3_key)
            storage_path = f"s3://{s3_bucket}/{s3_key}"
        except Exception:
            storage_path = local_path

        session.execute(
            text("""
            UPDATE concept_indexes
            SET is_materialized = true, materialized_at = :now, row_count = :cnt,
                column_stats = :stats, storage_path = :path, updated_at = :now
            WHERE index_id = :index_id
            """),
            {"index_id": index_id, "now": datetime.utcnow(), "cnt": len(df),
             "stats": json.dumps(column_stats), "path": storage_path}
        )
        session.commit()
        return index_id

    finally:
        session.close()


@op(
    config_schema={"rule_ids": list},
    out=DynamicOut(int),
)
def fan_out_rules(context: OpExecutionContext, index_done: int):
    """Emit each rule_id as a DynamicOutput after the index is materialized."""
    rule_ids = context.op_config["rule_ids"]
    context.log.info(f"Fanning out {len(rule_ids)} rules (index {index_done} done)")
    for rid in rule_ids:
        yield DynamicOutput(rid, mapping_key=f"rule_{rid}")


@op(
    required_resource_keys={"s3_storage"},
    out=Out(int),
)
def materialize_rule_op(context: OpExecutionContext, rule_id: int) -> int:
    """Op wrapper: materialize a single rule and return its rule_id."""
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
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
            raise ValueError(f"Rule {rule_id} not found")

        from backend.utils.resolve_entities import resolve_entity_ids
        df_index = resolve_entity_ids(rule_row.index_id, session)

        index_column = rule_row.index_column or df_index.columns[0]
        index_values = df_index[index_column].unique().tolist()

        password = decrypt_password(rule_row.encrypted_password)
        external_conn_str = build_conn_string(
            rule_row.connection_type, rule_row.user, password,
            rule_row.host, rule_row.port, rule_row.database,
        )
        external_engine = create_engine(external_conn_str)

        with external_engine.connect() as ext_conn:
            sample_val = index_values[0] if index_values else ""
            pg_type = "TEXT" if isinstance(sample_val, str) else "BIGINT"
            ext_conn.execute(text(f"CREATE TEMP TABLE _tippers_entity_ids (entity_id {pg_type}) ON COMMIT DROP"))

            BATCH_SIZE = 1000
            for i in range(0, len(index_values), BATCH_SIZE):
                batch = index_values[i:i + BATCH_SIZE]
                values_clause = ", ".join([f"(:v{j})" for j in range(len(batch))])
                params = {f"v{j}": v for j, v in enumerate(batch)}
                ext_conn.execute(text(f"INSERT INTO _tippers_entity_ids (entity_id) VALUES {values_clause}"), params)

            rendered_sql = rule_row.sql_query.replace(":index_values", "(SELECT entity_id FROM _tippers_entity_ids)")
            if rule_row.query_template_params:
                rendered_sql = Template(rendered_sql).render(**rule_row.query_template_params)

            df_features = pd.read_sql(text(rendered_sql), ext_conn)

        column_stats = compute_column_stats(df_features)
        local_path = get_storage_path("rule", rule_id)
        df_features.to_parquet(local_path, index=False)

        s3_client = context.resources.s3_storage.get_client()
        s3_bucket = context.resources.s3_storage.bucket_name
        s3_key = f"rules/rule_{rule_id}.parquet"

        try:
            s3_client.upload_file(local_path, s3_bucket, s3_key)
            storage_path = f"s3://{s3_bucket}/{s3_key}"
        except Exception:
            storage_path = local_path

        session.execute(
            text("""
            UPDATE concept_rules
            SET is_materialized = true, materialized_at = :now, row_count = :cnt,
                column_stats = :stats, storage_path = :path, updated_at = :now
            WHERE r_id = :rule_id
            """),
            {"rule_id": rule_id, "now": datetime.utcnow(), "cnt": len(df_features),
             "stats": json.dumps(column_stats), "path": storage_path}
        )
        session.commit()
        context.log.info(f"Rule {rule_id}: {len(df_features)} rows")
        return rule_id

    finally:
        session.close()


@op(
    config_schema={"job_id": int},
    required_resource_keys={"s3_storage"},
    out=Out(dict),
)
def run_snorkel_op(context: OpExecutionContext, rule_results: List[int]) -> dict:
    """Op wrapper: run Snorkel training after all rules are materialized.

    Delegates to the existing snorkel_training asset logic by triggering it
    via the Dagster client (keeps logic DRY).
    """
    job_id = context.op_config["job_id"]
    context.log.info(f"All {len(rule_results)} rules done. Running Snorkel job {job_id}")

    from backend.utils.dagster_client import get_dagster_client

    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="snorkel_training_pipeline",
        run_config={"ops": {"snorkel_training": {"config": {"job_id": job_id}}}}
    )

    return {"job_id": job_id, "dagster_run_id": result["run_id"]}


@graph
def snorkel_pipeline_graph():
    """Graph: materialize index → fan out rules (parallel) → run Snorkel."""
    idx = materialize_index_op()
    rules = fan_out_rules(idx).map(materialize_rule_op).collect()
    return run_snorkel_op(rules)
