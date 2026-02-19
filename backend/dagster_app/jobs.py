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

# Occupancy dataset computation job
occupancy_dataset_job = define_asset_job(
    name="occupancy_dataset_job",
    selection=AssetSelection.keys("occupancy_dataset")
)
