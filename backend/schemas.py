"""Pydantic schemas for request/response validation."""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


# ============================================================================
# Concept Schemas
# ============================================================================

class ConceptCreate(BaseModel):
    """Schema for creating a concept."""
    name: str = Field(..., description="Unique concept name")
    description: Optional[str] = Field(None, description="Concept description")


class ConceptUpdate(BaseModel):
    """Schema for updating a concept."""
    name: Optional[str] = None
    description: Optional[str] = None


class ConceptResponse(BaseModel):
    """Schema for concept response."""
    c_id: int
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# Concept Value Schemas
# ============================================================================

class ConceptValueCreate(BaseModel):
    """Schema for creating a concept value (label)."""
    name: str = Field(..., description="Label name (e.g., HIGH_VALUE, LOW_VALUE)")
    description: Optional[str] = Field(None, description="Label description")
    display_order: Optional[int] = Field(None, description="Display order in UI")
    level: int = Field(default=1, description="Classification level (1=top-level, 2=sub-level, etc.)")


class ConceptValueUpdate(BaseModel):
    """Schema for updating a concept value."""
    name: Optional[str] = None
    description: Optional[str] = None
    display_order: Optional[int] = None
    level: Optional[int] = None


class ConceptValueResponse(BaseModel):
    """Schema for concept value response."""
    cv_id: int
    c_id: int
    name: str
    description: Optional[str]
    display_order: Optional[int]
    level: int = 1
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# Database Connection Schemas
# ============================================================================

class DatabaseConnectionCreate(BaseModel):
    """Schema for creating a database connection."""
    name: str = Field(..., description="Unique connection name")
    connection_type: str = Field(..., description="Database type (postgresql, mysql, etc.)")
    host: str
    port: int
    database: str
    user: str
    password: str = Field(..., description="Password (will be encrypted)")


class DatabaseConnectionUpdate(BaseModel):
    """Schema for updating a database connection."""
    name: Optional[str] = None
    connection_type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None


class DatabaseConnectionResponse(BaseModel):
    """Schema for database connection response."""
    conn_id: int
    name: str
    connection_type: str
    host: str
    port: int
    database: str
    user: str
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# Index Schemas
# ============================================================================

class IndexCreate(BaseModel):
    """Schema for creating an index."""
    name: str = Field(..., description="Index name")
    conn_id: int = Field(..., description="Database connection ID")
    sql_query: str = Field(..., description="SQL query to execute")
    query_template_params: Optional[Dict[str, Any]] = Field(None, description="Jinja2 template parameters")
    partition_type: Optional[str] = Field(None, description="Partition type: time, id_range, categorical")
    partition_config: Optional[Dict[str, Any]] = Field(None, description="Partition configuration")


class IndexUpdate(BaseModel):
    """Schema for updating an index."""
    name: Optional[str] = None
    conn_id: Optional[int] = None
    sql_query: Optional[str] = None
    query_template_params: Optional[Dict[str, Any]] = None
    partition_type: Optional[str] = None
    partition_config: Optional[Dict[str, Any]] = None


class IndexResponse(BaseModel):
    """Schema for index response."""
    index_id: int
    c_id: int
    conn_id: Optional[int]
    name: str
    sql_query: str
    query_template_params: Optional[Dict[str, Any]]
    partition_type: Optional[str]
    partition_config: Optional[Dict[str, Any]]
    storage_path: Optional[str]
    is_materialized: bool
    materialized_at: Optional[datetime]
    row_count: Optional[int]
    column_stats: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IndexMaterializeResponse(BaseModel):
    """Schema for materialization response."""
    index_id: int
    dagster_run_id: str
    status: str


# ============================================================================
# Rule Schemas
# ============================================================================

class RuleCreate(BaseModel):
    """Schema for creating a rule."""
    name: str = Field(..., description="Rule name")
    index_id: int = Field(..., description="Index ID to use as sample set")
    sql_query: str = Field(..., description="SQL query with :index_values placeholder (e.g., WHERE user_id IN (:index_values))")
    index_column: Optional[str] = Field(None, description="Column from index to extract for filtering (e.g., 'user_id'). Defaults to first column if not specified.")
    query_template_params: Optional[Dict[str, Any]] = Field(None, description="Optional additional Jinja2 template parameters")
    partition_type: Optional[str] = Field(None, description="Partition type: time, id_range, categorical")
    partition_config: Optional[Dict[str, Any]] = Field(None, description="Partition configuration")


class RuleUpdate(BaseModel):
    """Schema for updating a rule."""
    name: Optional[str] = None
    index_id: Optional[int] = None
    sql_query: Optional[str] = None
    index_column: Optional[str] = None
    query_template_params: Optional[Dict[str, Any]] = None
    partition_type: Optional[str] = None
    partition_config: Optional[Dict[str, Any]] = None


class RuleResponse(BaseModel):
    """Schema for rule response."""
    r_id: int
    c_id: int
    index_id: int
    name: str
    sql_query: str
    index_column: Optional[str]
    query_template_params: Optional[Dict[str, Any]]
    partition_type: Optional[str]
    partition_config: Optional[Dict[str, Any]]
    storage_path: Optional[str]
    is_materialized: bool
    materialized_at: Optional[datetime]
    row_count: Optional[int]
    column_stats: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class RuleMaterializeResponse(BaseModel):
    """Schema for rule materialization response."""
    r_id: int
    dagster_run_id: str
    status: str


# ============================================================================
# Labeling Function Schemas
# ============================================================================

class LabelingFunctionCreate(BaseModel):
    """Schema for creating a labeling function (custom Python only)."""
    name: str = Field(..., description="LF name")
    rule_id: int = Field(..., description="Rule ID that provides features")
    applicable_cv_ids: List[int] = Field(..., description="Concept value IDs this LF can vote on")
    code: Optional[str] = Field(None, description="Python code for labeling function. If omitted, a template is auto-generated.")
    allowed_imports: List[str] = Field(default_factory=list, description="Allowed import modules")
    parent_lf_id: Optional[int] = Field(None, description="Parent LF ID for versioning")


class LabelingFunctionUpdate(BaseModel):
    """Schema for updating a labeling function."""
    name: Optional[str] = None
    is_active: Optional[bool] = None
    lf_config: Optional[Dict[str, Any]] = None
    applicable_cv_ids: Optional[List[int]] = None


class LabelingFunctionResponse(BaseModel):
    """Schema for labeling function response."""
    lf_id: int
    c_id: int
    applicable_cv_ids: List[int]
    rule_id: int
    name: str
    version: int
    parent_lf_id: Optional[int]
    lf_type: str
    lf_config: Dict[str, Any]
    is_active: bool
    requires_approval: bool
    deprecated_at: Optional[datetime]
    deprecated_by_lf_id: Optional[int]
    estimated_accuracy: Optional[float]
    coverage: Optional[float]
    conflicts: Optional[int]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class LabelingFunctionVersionCreate(BaseModel):
    """Schema for creating a new version of an LF."""
    lf_config: Dict[str, Any] = Field(..., description="Updated LF configuration")
    name: Optional[str] = Field(None, description="Optional new name for this version")


# ============================================================================
# Snorkel Schemas
# ============================================================================

class SnorkelConfig(BaseModel):
    """Snorkel training configuration."""
    epochs: int = Field(default=100, description="Number of training epochs")
    lr: float = Field(default=0.01, description="Learning rate")
    sample_size: Optional[int] = Field(None, description="Sample size for training")
    output_type: str = Field(default="softmax", description="Output type: softmax or hard_labels")


class SnorkelRunRequest(BaseModel):
    """Schema for triggering Snorkel training."""
    selectedIndex: int = Field(..., description="Index ID to use as dataset")
    selectedRules: List[int] = Field(default_factory=list, description="Rule IDs to include")
    selectedLFs: List[int] = Field(..., description="Labeling function IDs to apply")
    snorkel: SnorkelConfig


class SnorkelJobResponse(BaseModel):
    """Schema for Snorkel job response."""
    job_id: int
    c_id: int
    index_id: int
    rule_ids: Optional[List[int]]
    lf_ids: Optional[List[int]]
    config: Optional[Dict[str, Any]]
    output_type: str
    dagster_run_id: Optional[str]
    status: str
    result_path: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class SnorkelPrediction(BaseModel):
    """Individual prediction result."""
    sample_id: int
    predicted_class: int
    probs: Optional[List[float]] = None
    confidence: Optional[float] = None


class SnorkelResultsResponse(BaseModel):
    """Schema for Snorkel training results."""
    job_id: int
    output_type: str
    predictions: List[SnorkelPrediction]
    summary: Dict[str, Any]


# ============================================================================
# Catalog Schemas
# ============================================================================

class CatalogIndexItem(BaseModel):
    """Catalog item for index."""
    id: int
    name: str
    is_materialized: bool
    row_count: Optional[int]
    storage_path: Optional[str]


class CatalogRuleItem(BaseModel):
    """Catalog item for rule."""
    id: int
    name: str
    is_materialized: bool
    row_count: Optional[int]
    storage_path: Optional[str]


class AssetCatalogResponse(BaseModel):
    """Asset catalog response."""
    indexes: List[CatalogIndexItem]
    rules: List[CatalogRuleItem]


# ============================================================================
# Dagster Schemas
# ============================================================================

class DagsterRunStatusResponse(BaseModel):
    """Schema for Dagster run status."""
    run_id: str
    status: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    error_message: Optional[str]


# ============================================================================
# Feature Schemas
# ============================================================================

class FeatureCreate(BaseModel):
    """Schema for creating a feature."""
    name: str = Field(..., description="Feature name")
    index_id: int = Field(..., description="Index ID to use as sample set")
    sql_query: str = Field(..., description="SQL query with :index_values placeholder")
    index_column: Optional[str] = Field(None, description="Column from index for filtering (defaults to first column)")
    columns: Optional[List[str]] = Field(None, description="Expected output column names")
    query_template_params: Optional[Dict[str, Any]] = Field(None, description="Jinja2 template parameters")
    description: Optional[str] = Field(None, description="Feature description")
    level: Optional[int] = Field(None, description="UI display level")
    partition_type: Optional[str] = Field(None, description="Partition type")
    partition_config: Optional[Dict[str, Any]] = Field(None, description="Partition configuration")


class FeatureUpdate(BaseModel):
    """Schema for updating a feature."""
    name: Optional[str] = None
    sql_query: Optional[str] = None
    index_column: Optional[str] = None
    columns: Optional[List[str]] = None
    query_template_params: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    level: Optional[int] = None
    partition_type: Optional[str] = None
    partition_config: Optional[Dict[str, Any]] = None


class FeatureResponse(BaseModel):
    """Schema for feature response."""
    feature_id: int
    c_id: int
    index_id: int
    name: str
    description: Optional[str]
    sql_query: str
    index_column: Optional[str]
    columns: Optional[List[str]]
    query_template_params: Optional[Dict[str, Any]]
    level: Optional[int]
    partition_type: Optional[str]
    partition_config: Optional[Dict[str, Any]]
    storage_path: Optional[str]
    is_materialized: bool
    materialized_at: Optional[datetime]
    row_count: Optional[int]
    column_stats: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FeatureMaterializeResponse(BaseModel):
    """Schema for feature materialization response."""
    feature_id: int
    dagster_run_id: str
    status: str


# ============================================================================
# Classifier Schemas
# ============================================================================

class ClassifierConfig(BaseModel):
    """Classifier training configuration."""
    threshold_method: str = Field(default="max_confidence", description="'entropy' or 'max_confidence'")
    threshold_value: float = Field(default=0.7, description="Confidence threshold for filtering")
    min_labels_per_class: int = Field(default=10, description="Minimum samples per class after filtering")
    imbalance_factor: float = Field(default=3.0, description="Max ratio of largest to smallest class")
    test_size: float = Field(default=0.2, description="Test set fraction")
    random_state: int = Field(default=42, description="Random seed")
    n_estimators: int = Field(default=100, description="Number of estimators for ensemble models")
    max_depth: Optional[int] = Field(default=None, description="Max tree depth (None for unlimited)")


class ClassifierRunRequest(BaseModel):
    """Schema for triggering classifier training."""
    snorkel_job_id: int = Field(..., description="Snorkel job ID whose results to use")
    feature_ids: List[int] = Field(..., description="Feature IDs to join as training data")
    config: ClassifierConfig


class ClassifierJobResponse(BaseModel):
    """Schema for classifier job response."""
    job_id: int
    c_id: int
    snorkel_job_id: int
    feature_ids: List[int]
    config: Dict[str, Any]
    dagster_run_id: Optional[str]
    status: str
    result_path: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True
