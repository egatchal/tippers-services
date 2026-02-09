"""Dagster job definitions for weak supervision pipeline."""
from dagster import define_asset_job, AssetSelection

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

# Full Snorkel training pipeline (all three assets)
snorkel_training_pipeline = define_asset_job(
    name="snorkel_training_pipeline",
    selection=AssetSelection.groups("data_materialization", "feature_engineering", "weak_supervision")
)
