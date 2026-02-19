from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, ARRAY, JSON, Float, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from backend.db.session import Base


class Concept(Base):
    """Concepts table - the top-level entity for weak supervision."""
    __tablename__ = "concepts"

    c_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class ConceptValue(Base):
    """Concept values table - represents the labels/classes for a concept."""
    __tablename__ = "concept_values"

    cv_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    display_order = Column(Integer, nullable=True)
    level = Column(Integer, default=1, nullable=False, server_default='1')
    created_at = Column(TIMESTAMP, server_default=func.now())


class DatabaseConnection(Base):
    """Database connections table."""
    __tablename__ = "database_connections"

    conn_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    connection_type = Column(String(50), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    database = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    encrypted_password = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())


class ConceptIndex(Base):
    """Concept indexes table - raw data source for Snorkel."""
    __tablename__ = "concept_indexes"

    index_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    conn_id = Column(Integer, ForeignKey('database_connections.conn_id'), nullable=True)
    name = Column(String(255), nullable=False)
    sql_query = Column(Text, nullable=False)
    query_template_params = Column(JSON, nullable=True)
    partition_type = Column(String(50), nullable=True)
    partition_config = Column(JSON, nullable=True)
    storage_path = Column(String(500), nullable=True)
    is_materialized = Column(Boolean, default=False)
    materialized_at = Column(TIMESTAMP, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_stats = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class ConceptRule(Base):
    """Concept rules/features table - computes features that labeling functions use."""
    __tablename__ = "concept_rules"

    r_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    index_id = Column(Integer, ForeignKey('concept_indexes.index_id'), nullable=False)
    name = Column(String(255), nullable=False)
    sql_query = Column(Text, nullable=False)
    query_template_params = Column(JSON, nullable=True)
    index_column = Column(String(255), nullable=True)  # Column from index to use for filtering (e.g., 'user_id')

    partition_type = Column(String(50), nullable=True)
    partition_config = Column(JSON, nullable=True)
    storage_path = Column(String(500), nullable=True)
    is_materialized = Column(Boolean, default=False)
    materialized_at = Column(TIMESTAMP, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_stats = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class LabelingFunction(Base):
    """Labeling functions table - voting logic that uses rule features (versioned)."""
    __tablename__ = "labeling_functions"

    lf_id = Column(Integer, primary_key=True, autoincrement=True)

    # Core references
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    applicable_cv_ids = Column(ARRAY(Integer), nullable=False)
    rule_id = Column(Integer, ForeignKey('concept_rules.r_id'), nullable=False)

    name = Column(String(255), nullable=False)

    # Versioning support
    version = Column(Integer, default=1, nullable=False)
    parent_lf_id = Column(Integer, ForeignKey('labeling_functions.lf_id'), nullable=True)

    # LF configuration
    lf_type = Column(String(50), nullable=False)
    lf_config = Column(JSON, nullable=False)
    # Example: {
    #   "feature_conditions": {
    #     "total_spent": "> 1000",
    #     "purchase_count": "> 5"
    #   }
    # }

    # Status and lifecycle
    is_active = Column(Boolean, default=True)
    requires_approval = Column(Boolean, default=False)
    deprecated_at = Column(TIMESTAMP, nullable=True)
    deprecated_by_lf_id = Column(Integer, ForeignKey('labeling_functions.lf_id'), nullable=True)

    # Performance metrics (populated after Snorkel evaluation)
    estimated_accuracy = Column(Float, nullable=True)
    coverage = Column(Float, nullable=True)
    conflicts = Column(Integer, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class SnorkelJob(Base):
    """Snorkel training jobs table - orchestrates the weak supervision pipeline."""
    __tablename__ = "snorkel_jobs"

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    index_id = Column(Integer, ForeignKey('concept_indexes.index_id'), nullable=False)

    # References to rules (for feature computation) and LFs (for voting)
    rule_ids = Column(ARRAY(Integer), nullable=True)
    lf_ids = Column(ARRAY(Integer), nullable=True)

    # Snorkel configuration
    config = Column(JSON, nullable=True)
    # Example: {
    #   "learning_rate": 0.01,
    #   "epochs": 100,
    #   "class_balance": [0.3, 0.4, 0.3]
    # }

    output_type = Column(String(50), default='hard_labels')
    dagster_run_id = Column(String(255), nullable=True)
    status = Column(String(50), default='PENDING')
    result_path = Column(String(500), nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    completed_at = Column(TIMESTAMP, nullable=True)


class ConceptFeature(Base):
    """Concept features table - simple SQL aggregations used as classifier training inputs."""
    __tablename__ = "concept_features"

    feature_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    index_id = Column(Integer, ForeignKey('concept_indexes.index_id'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    sql_query = Column(Text, nullable=False)
    index_column = Column(String(255), nullable=True)
    columns = Column(ARRAY(String), nullable=True)
    query_template_params = Column(JSON, nullable=True)
    level = Column(Integer, nullable=True)
    partition_type = Column(String(50), nullable=True)
    partition_config = Column(JSON, nullable=True)
    storage_path = Column(String(500), nullable=True)
    is_materialized = Column(Boolean, default=False)
    materialized_at = Column(TIMESTAMP, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_stats = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class ClassifierJob(Base):
    """Classifier training jobs table - trains classifiers on Snorkel-labeled data with features."""
    __tablename__ = "classifier_jobs"

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    c_id = Column(Integer, ForeignKey('concepts.c_id'), nullable=False)
    snorkel_job_id = Column(Integer, ForeignKey('snorkel_jobs.job_id'), nullable=False)
    feature_ids = Column(ARRAY(Integer), nullable=False)

    config = Column(JSON, nullable=False)

    dagster_run_id = Column(String(255), nullable=True)
    status = Column(String(50), default='PENDING')
    result_path = Column(String(500), nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    completed_at = Column(TIMESTAMP, nullable=True)

class OccupancyDataset(Base):
    """Occupancy datasets table - computes space occupancy over time from WiFi session data."""
    __tablename__ = "occupancy_datasets"

    dataset_id       = Column(Integer, primary_key=True, autoincrement=True)
    name             = Column(String(255), nullable=False)
    description      = Column(Text, nullable=True)
    root_space_id    = Column(Integer, nullable=False)
    start_time       = Column(TIMESTAMP, nullable=False)
    end_time         = Column(TIMESTAMP, nullable=False)
    interval_seconds = Column(Integer, nullable=False)
    chunk_days       = Column(Integer, nullable=True)
    status           = Column(String(50), default='PENDING')
    dagster_run_id   = Column(String(255), nullable=True)
    storage_path     = Column(String(500), nullable=True)
    row_count        = Column(Integer, nullable=True)
    column_stats     = Column(JSON, nullable=True)
    error_message    = Column(Text, nullable=True)
    created_at       = Column(TIMESTAMP, server_default=func.now())
    completed_at     = Column(TIMESTAMP, nullable=True)


class HostedModel(Base):
    """
    Logical model identity for your app.

    MLflow stores the real model artifacts and versions.
    This table tracks the model name and minimal metadata you want in Postgres.
    """
    __tablename__ = "hosted_models"

    model_id = Column(Integer, primary_key=True, autoincrement=True)

    # For MVP single-tenant you can hardcode owner_id="default" in the API.
    # If you already have users/auth, set owner_id accordingly.
    owner_id = Column(String(255), nullable=False, index=True)

    # User-facing name (and typically also the MLflow registered model name)
    name = Column(String(255), nullable=False)

    description = Column(Text, nullable=True)
    activity = Column(String(64), nullable=False, server_default="occupancy")  # MVP default
    visibility = Column(String(32), nullable=False, server_default="private")  # private/public
    status = Column(String(32), nullable=False, server_default="ACTIVE")       # ACTIVE/ARCHIVED

    # MLflow Registered Model name
    mlflow_registered_name = Column(String(255), nullable=False, index=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    versions = relationship("HostedModelVersion", back_populates="model", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("owner_id", "name", name="uq_hosted_models_owner_name"),
    )


class HostedModelVersion(Base):
    """
    One row per MLflow Model Registry version.
    """
    __tablename__ = "hosted_model_versions"

    model_version_id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("hosted_models.model_id", ondelete="CASCADE"), nullable=False)

    # MLflow version number for the registered model
    mlflow_version = Column(Integer, nullable=False)

    # Where it came from
    mlflow_run_id = Column(String(255), nullable=True)
    artifact_uri = Column(String(500), nullable=True)

    # Cached stage (Production/Staging/Archived/None)
    stage = Column(String(64), nullable=True)

    status = Column(String(32), nullable=False, server_default="REGISTERED")   # REGISTERED/FAILED
    error_message = Column(Text, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())

    model = relationship("HostedModel", back_populates="versions")

    __table_args__ = (
        UniqueConstraint("model_id", "mlflow_version", name="uq_hosted_model_versions_model_mlflow_version"),
    )
