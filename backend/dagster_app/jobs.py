"""Dagster job definitions for weak supervision pipeline."""
import os
from dagster import define_asset_job, AssetSelection, multiprocess_executor
from backend.dagster_app.assets import occupancy_incremental_graph, snorkel_pipeline_graph
from backend.dagster_app.resources import s3_resource

# Job to materialize a single index
materialize_index_job = define_asset_job(
    name="materialize_index_job",
    selection=AssetSelection.keys("materialized_index")
)

# Job to materialize a single rule (requires index to be materialized first)
materialize_rule_job = define_asset_job(
    name="materialize_rule_job",
    selection=AssetSelection.keys("materialized_rule")
)

# Job to materialize a single feature (requires index to be materialized first)
materialize_feature_job = define_asset_job(
    name="materialize_feature_job",
    selection=AssetSelection.keys("materialized_feature")
)

# Snorkel training job (index and rules must already be materialized)
snorkel_training_pipeline = define_asset_job(
    name="snorkel_training_pipeline",
    selection=AssetSelection.keys("snorkel_training")
)

# Classifier training pipeline (requires Snorkel job completed + features materialized)
classifier_training_pipeline = define_asset_job(
    name="classifier_training_pipeline",
    selection=AssetSelection.keys("classifier_training")
)

# Snorkel full pipeline: index → rules (parallel) → Snorkel training
snorkel_pipeline_job = snorkel_pipeline_graph.to_job(
    name="snorkel_pipeline_job",
    resource_defs={"s3_storage": s3_resource},
)

# Incremental occupancy job — fan-out source chunks in parallel, then aggregate
occupancy_incremental_job = occupancy_incremental_graph.to_job(
    name="occupancy_incremental_job",
    resource_defs={"s3_storage": s3_resource},
    executor_def=multiprocess_executor.configured(
        {"max_concurrent": int(os.environ.get("OCCUPANCY_MAX_CONCURRENT", "8"))}
    ),
)
