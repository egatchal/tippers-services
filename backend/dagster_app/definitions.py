from dagster import Definitions, load_assets_from_modules
from backend.dagster_app import assets
from backend.dagster_app.resources import database_resource, s3_resource, mlflow_resource
from backend.dagster_app.jobs import (
    materialize_index_job,
    materialize_rule_job,
    snorkel_training_pipeline
)

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        materialize_index_job,
        materialize_rule_job,
        snorkel_training_pipeline
    ],
    resources={
        "database_connection": database_resource,
        "s3_storage": s3_resource,
        "mlflow": mlflow_resource
    }
)
