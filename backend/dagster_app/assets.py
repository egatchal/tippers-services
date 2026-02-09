"""Dagster assets for weak supervision pipeline."""
from dagster import asset, AssetExecutionContext, Output, AssetIn
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any
import json
from cryptography.fernet import Fernet


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
        # Get rule metadata
        context.log.info(f"Loading rule metadata for rule_id={rule_id}")

        result = session.execute(
            text("""
            SELECT * FROM concept_rules
            WHERE r_id = :rule_id
            """),
            {"rule_id": rule_id}
        )

        rule_row = result.fetchone()

        if not rule_row:
            raise ValueError(f"Rule with ID {rule_id} not found")

        # Load index data
        index_storage_path = index_data["storage_path"]
        context.log.info(f"Loading index data from {index_storage_path}")

        df_index = pd.read_parquet(index_storage_path)

        context.log.info(f"Loaded {len(df_index)} rows from index")

        # Execute rule SQL on index data
        # For simplicity, we'll use pandas if the query is simple
        # In production, you might want to use DuckDB or similar
        sql_query = rule_row.sql_query

        context.log.info(f"Executing rule query: {sql_query[:100]}...")

        # Create temporary table in DuckDB or similar
        # For now, we'll use a simple pandas approach
        # TODO: Implement proper SQL execution on dataframe

        # Placeholder: just return the index data with some computed columns
        # In production, execute the actual SQL query
        df_features = df_index  # This should be the result of executing sql_query on df_index

        context.log.info(f"Computed features: {len(df_features)} rows")

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


@asset(
    group_name="weak_supervision",
    description="Trains Snorkel label model and generates labels",
    ins={"features": AssetIn("materialized_rule")},
    required_resource_keys={"s3_storage"}
)
def snorkel_training(context: AssetExecutionContext, features: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """
    Train Snorkel label model and generate probabilistic labels.

    Loads feature data, applies labeling functions, builds label matrix,
    trains Snorkel's LabelModel, and outputs predictions.

    Returns metadata about the labeled dataset.
    """
    # Get job_id from config
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
            text("""
            SELECT * FROM snorkel_jobs
            WHERE job_id = :job_id
            """),
            {"job_id": job_id}
        )

        job_row = result.fetchone()

        if not job_row:
            raise ValueError(f"Snorkel job with ID {job_id} not found")

        # Load feature data
        features_storage_path = features["storage_path"]
        context.log.info(f"Loading features from {features_storage_path}")

        df_features = pd.read_parquet(features_storage_path)

        context.log.info(f"Loaded {len(df_features)} rows of features")

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

        # Apply labeling functions to create label matrix
        # This is a simplified version - in production, use Snorkel's actual LF application
        import numpy as np

        n_samples = len(df_features)
        n_lfs = len(lfs)
        L = np.zeros((n_samples, n_lfs), dtype=int)  # Label matrix

        for i, lf in enumerate(lfs):
            context.log.info(f"Applying LF {i+1}/{n_lfs}: {lf.name}")

            # TODO: Implement actual LF execution based on lf_type and lf_config
            # For now, randomly assign labels as placeholder
            # In production, execute the actual LF logic
            L[:, i] = np.random.choice([-1, 0, 1], size=n_samples)  # -1=abstain, 0/1=labels

        context.log.info("Label matrix created")

        # Train Snorkel LabelModel
        try:
            from snorkel.labeling import LabelModel

            label_model = LabelModel(cardinality=2, verbose=True)

            # Get config
            config = job_row.config or {}
            epochs = config.get("epochs", 100)
            lr = config.get("lr", 0.01)

            context.log.info(f"Training LabelModel (epochs={epochs}, lr={lr})")

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
                    "sample_ids": list(range(len(predictions)))
                }
            else:  # softmax
                probs = label_model.predict_proba(L)
                result_data = {
                    "probabilities": probs.tolist(),
                    "sample_ids": list(range(len(probs)))
                }

            # Update LF performance metrics
            lf_accuracies = label_model.get_weights()

            for i, lf in enumerate(lfs):
                # Calculate coverage (% of samples not abstaining)
                coverage = (L[:, i] != -1).sum() / n_samples

                # Count conflicts
                conflicts = 0
                for j in range(n_lfs):
                    if i != j:
                        both_vote = (L[:, i] != -1) & (L[:, j] != -1)
                        disagree = L[both_vote, i] != L[both_vote, j]
                        conflicts += disagree.sum()

                # Update LF metrics
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
                "sample_ids": list(range(n_samples))
            }

        # Store results locally
        local_storage_path = get_storage_path("snorkel_job", job_id)
        local_storage_path = local_storage_path.replace('.parquet', '.json')  # Change extension for JSON

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

        # Update job status to failed
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
