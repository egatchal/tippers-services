"""Dagster job definitions for weak supervision pipeline."""
import os
from dagster import define_asset_job, AssetSelection, multiprocess_executor
from backend.dagster_app.assets import (
    occupancy_dataset_graph,
    source_chunk_graph,
    derived_chunk_graph,
)
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

# Occupancy dataset computation job (graph-based with DynamicOutput fan-out)
occupancy_dataset_job = occupancy_dataset_graph.to_job(
    name="occupancy_dataset_job",
    resource_defs={"s3_storage": s3_resource},
    executor_def=multiprocess_executor.configured({
        "max_concurrent": int(os.getenv("DAGSTER_MAX_CONCURRENT", "8"))
    }),
)

# Per-space-chunk jobs (one Dagster run per chunk; config_schema carries chunk_id)
materialize_source_chunk_job = source_chunk_graph.to_job(
    name="materialize_source_chunk_job",
    resource_defs={"s3_storage": s3_resource},
)

materialize_derived_chunk_job = derived_chunk_graph.to_job(
    name="materialize_derived_chunk_job",
    resource_defs={"s3_storage": s3_resource},
)
