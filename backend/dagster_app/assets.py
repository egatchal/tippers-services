"""Dagster assets for weak supervision pipeline."""
from dagster import asset, AssetExecutionContext, Output, AssetIn
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import os
from datetime import datetime
from typing import Dict, Any
import json
from cryptography.fernet import Fernet
from jinja2 import Template


def get_db_engine():
    """Get database engine from environment."""
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/tippers")
    return create_engine(database_url)


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


@asset(
    group_name="data_materialization",
    description="Materializes index data from external database",
    required_resource_keys={"s3_storage"}
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
                storage_path = :storage_path,
                updated_at = :now
            WHERE index_id = :index_id
            """),
            {
                "index_id": index_id,
                "now": datetime.utcnow(),
                "row_count": len(df),
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
    ins={"index_data": AssetIn("materialized_index")},
    required_resource_keys={"s3_storage"}
)
def materialized_rule(context: AssetExecutionContext, index_data: Dict[str, Any]) -> Output[Dict[str, Any]]:
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
                storage_path = :storage_path,
                updated_at = :now
            WHERE r_id = :rule_id
            """),
            {
                "rule_id": rule_id,
                "now": datetime.utcnow(),
                "row_count": len(df_features),
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


def apply_custom_lf(df: pd.DataFrame, lf_config: dict, valid_cv_ids: set, cv_id_to_index: dict, context) -> np.ndarray:
    """
    Execute custom Python labeling function.

    Safely executes user-provided code against feature DataFrame.
    Returns values are remapped from cv_ids to 0-indexed class labels using cv_id_to_index.
    -1 (ABSTAIN) is preserved as-is.

    Args:
        df: Feature DataFrame
        lf_config: LF configuration with code and allowed_imports
        valid_cv_ids: Set of valid cv_ids the LF may return
        cv_id_to_index: Mapping from cv_id to 0-indexed class label
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

    # Get the labeling function (should be named 'labeling_function')
    labeling_function = safe_globals.get("labeling_function")

    if not labeling_function:
        raise ValueError("Custom code must define a function named 'labeling_function'")

    # Apply to each row
    labels = np.full(len(df), -1, dtype=int)
    for idx, row in df.iterrows():
        try:
            label = labeling_function(row)
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
    required_resource_keys={"s3_storage"}
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
        all_cv_ids = set()
        for lf in lfs:
            all_cv_ids.update(lf.applicable_cv_ids)

        sorted_cv_ids = sorted(all_cv_ids)
        cv_id_to_index = {cv_id: idx for idx, cv_id in enumerate(sorted_cv_ids)}
        cardinality = len(sorted_cv_ids)

        context.log.info(f"CV-ID-to-index mapping: {cv_id_to_index} (cardinality={cardinality})")

        # Load and join rule feature DataFrames for each LF
        s3_resource = context.resources.s3_storage
        rule_dfs = {}
        for lf in lfs:
            if lf.rule_id not in rule_dfs:
                context.log.info(f"Loading features for rule_id={lf.rule_id}")
                rule_dfs[lf.rule_id] = load_rule_features(lf.rule_id, session, s3_resource, context)

        # Join all rule feature DataFrames on index (outer join to keep all samples)
        rule_ids = list(rule_dfs.keys())
        df_features = rule_dfs[rule_ids[0]]
        for rid in rule_ids[1:]:
            df_features = df_features.join(rule_dfs[rid], how="outer", rsuffix=f"_rule{rid}")

        context.log.info(f"Combined feature DataFrame: {len(df_features)} rows, {len(df_features.columns)} columns")

        # Apply labeling functions to create label matrix
        n_samples = len(df_features)
        n_lfs = len(lfs)
        L = np.full((n_samples, n_lfs), -1, dtype=int)  # Label matrix, default abstain

        for i, lf in enumerate(lfs):
            context.log.info(f"Applying LF {i+1}/{n_lfs}: {lf.name}")

            # Load the specific rule's features for this LF
            lf_features = rule_dfs[lf.rule_id]
            valid_cv_ids = set(lf.applicable_cv_ids)

            try:
                L[:, i] = apply_custom_lf(lf_features, lf.lf_config, valid_cv_ids, cv_id_to_index, context)
            except Exception as e:
                context.log.error(f"Error applying LF {lf.name}: {str(e)}")
                L[:, i] = -1

        context.log.info("Label matrix created")

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

            # Generate predictions
            output_type = job_row.output_type

            if output_type == "hard_labels":
                predictions = label_model.predict(L)
                result_data = {
                    "labels": predictions.tolist(),
                    "sample_ids": list(range(len(predictions))),
                    "cv_id_to_index": cv_id_to_index
                }
            else:  # softmax
                probs = label_model.predict_proba(L)
                result_data = {
                    "probabilities": probs.tolist(),
                    "sample_ids": list(range(len(probs))),
                    "cv_id_to_index": cv_id_to_index
                }

            # Update LF performance metrics
            lf_accuracies = label_model.get_weights()

            for i, lf in enumerate(lfs):
                coverage = (L[:, i] != -1).sum() / n_samples

                conflicts = 0
                for j in range(n_lfs):
                    if i != j:
                        both_vote = (L[:, i] != -1) & (L[:, j] != -1)
                        disagree = L[both_vote, i] != L[both_vote, j]
                        conflicts += disagree.sum()

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
                        "accuracy": float(lf_accuracies[i]) if i < len(lf_accuracies) else None,
                        "coverage": float(coverage),
                        "conflicts": int(conflicts),
                        "now": datetime.utcnow()
                    }
                )

            session.commit()
            context.log.info("LF metrics updated")

        except ImportError:
            context.log.warning("Snorkel not installed. Using placeholder predictions.")
            result_data = {
                "labels": [0] * n_samples,
                "sample_ids": list(range(n_samples)),
                "cv_id_to_index": cv_id_to_index
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
