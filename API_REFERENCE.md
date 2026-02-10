# Tippers Services - API Reference

Complete API reference for all endpoints. Base URL: `http://localhost:8000`

## Table of Contents

1. [Health Check](#health-check)
2. [Database Connections](#database-connections)
3. [Concepts](#concepts)
4. [Concept Values](#concept-values)
5. [Indexes](#indexes)
6. [Rules](#rules)
7. [Labeling Functions](#labeling-functions)
8. [Asset Catalog](#asset-catalog)
9. [Snorkel Training](#snorkel-training)
10. [Dagster Integration](#dagster-integration)
11. [Common Patterns](#common-patterns)

---

## Health Check

### Application Health
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy"
}
```

---

## Database Connections

Manage database connections for querying external databases. Passwords are encrypted at rest.

### Create Connection
```http
POST /database-connections
```

**Request Body:**
```json
{
  "name": "tippers_db",
  "connection_type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "tippers",
  "user": "dbuser",
  "password": "secret123"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique connection name |
| `connection_type` | string | Yes | Database type (`postgresql`, `mysql`) |
| `host` | string | Yes | Database host |
| `port` | integer | Yes | Database port |
| `database` | string | Yes | Database name |
| `user` | string | Yes | Database user |
| `password` | string | Yes | Password (encrypted at rest) |

**Response (201):**
```json
{
  "conn_id": 1,
  "name": "tippers_db",
  "connection_type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "tippers",
  "user": "dbuser",
  "created_at": "2024-01-15T10:00:00Z"
}
```

### List Connections
```http
GET /database-connections?skip=0&limit=100
```

**Response (200):** Array of `DatabaseConnectionResponse`.

### Get Connection
```http
GET /database-connections/{conn_id}
```

**Response (200):** Single `DatabaseConnectionResponse`.

### Update Connection
```http
PATCH /database-connections/{conn_id}
```

**Request Body (partial update):**
```json
{
  "host": "new-host.example.com",
  "port": 5433
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New connection name |
| `connection_type` | string | No | New database type |
| `host` | string | No | New host |
| `port` | integer | No | New port |
| `database` | string | No | New database name |
| `user` | string | No | New user |
| `password` | string | No | New password |

**Response (200):** Updated `DatabaseConnectionResponse`.

### Delete Connection
```http
DELETE /database-connections/{conn_id}
```

**Response:** `204 No Content`

### Test Connection
```http
POST /database-connections/{conn_id}/test
```

**Response (200):**
```json
{
  "status": "success",
  "message": "Connection successful"
}
```

On failure:
```json
{
  "status": "failed",
  "message": "OperationalError: connection refused"
}
```

---

## Concepts

Top-level entities for weak supervision. Each concept has its own set of values, indexes, rules, labeling functions, and Snorkel jobs.

### Create Concept
```http
POST /concepts
```

**Request Body:**
```json
{
  "name": "device_classification",
  "description": "Classify devices by usage pattern"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique concept name |
| `description` | string | No | Concept description |

**Response (201):**
```json
{
  "c_id": 1,
  "name": "device_classification",
  "description": "Classify devices by usage pattern",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### List Concepts
```http
GET /concepts?skip=0&limit=100
```

**Response (200):** Array of `ConceptResponse`.

### Get Concept
```http
GET /concepts/{c_id}
```

**Response (200):** Single `ConceptResponse`.

### Update Concept
```http
PATCH /concepts/{c_id}
```

**Request Body (partial update):**
```json
{
  "description": "Updated description"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New concept name (must be unique) |
| `description` | string | No | New description |

**Response (200):** Updated `ConceptResponse`.

### Delete Concept
```http
DELETE /concepts/{c_id}
```

Cascade deletes all related concept values, indexes, rules, labeling functions, and Snorkel jobs.

**Response:** `204 No Content`

---

## Concept Values

Labels/classes for a concept. Each concept value represents one possible classification outcome.

### Create Concept Value
```http
POST /concepts/{c_id}/values
```

**Request Body:**
```json
{
  "name": "STATIC",
  "description": "Intermittent static device",
  "display_order": 1
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Label name (e.g., `STATIC`, `LAPTOP`, `PHONE`) |
| `description` | string | No | Label description |
| `display_order` | integer | No | Display order in UI |

**Response (201):**
```json
{
  "cv_id": 1,
  "c_id": 1,
  "name": "STATIC",
  "description": "Intermittent static device",
  "display_order": 1,
  "created_at": "2024-01-15T10:00:00Z"
}
```

### List Concept Values
```http
GET /concepts/{c_id}/values
```

Returns values ordered by `display_order`, then `cv_id`.

**Response (200):** Array of `ConceptValueResponse`.

### Get Concept Value
```http
GET /concepts/{c_id}/values/{cv_id}
```

**Response (200):** Single `ConceptValueResponse`.

### Update Concept Value
```http
PATCH /concepts/{c_id}/values/{cv_id}
```

**Request Body (partial update):**
```json
{
  "description": "Updated description",
  "display_order": 2
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New label name (must be unique within concept) |
| `description` | string | No | New description |
| `display_order` | integer | No | New display order |

**Response (200):** Updated `ConceptValueResponse`.

### Delete Concept Value
```http
DELETE /concepts/{c_id}/values/{cv_id}
```

**Response:** `204 No Content`

---

## Indexes

The **sampling layer** that defines which records to work with. Indexes are materialized once and reused by multiple rules.

### Create Index
```http
POST /concepts/{c_id}/indexes
```

**Request Body:**
```json
{
  "name": "unique_mac_addresses",
  "conn_id": 1,
  "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 5000"
}
```

**With template parameters:**
```json
{
  "name": "user_activity_2024",
  "conn_id": 1,
  "sql_query": "SELECT user_id FROM user_events WHERE year = {{ year }}",
  "query_template_params": { "year": 2024 },
  "partition_type": "time",
  "partition_config": { "column": "activity_date", "granularity": "monthly" }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique index name within the concept |
| `conn_id` | integer | Yes | Database connection ID |
| `sql_query` | string | Yes | SQL SELECT or WITH query |
| `query_template_params` | object | No | Jinja2 template parameters |
| `partition_type` | string | No | Partition type: `time`, `id_range`, `categorical` |
| `partition_config` | object | No | Partition configuration |

**Response (201):**
```json
{
  "index_id": 1,
  "c_id": 1,
  "conn_id": 1,
  "name": "unique_mac_addresses",
  "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 5000",
  "query_template_params": null,
  "partition_type": null,
  "partition_config": null,
  "storage_path": null,
  "is_materialized": false,
  "materialized_at": null,
  "row_count": null,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### List Indexes
```http
GET /concepts/{c_id}/indexes?skip=0&limit=100
```

**Response (200):** Array of `IndexResponse`.

### Get Index
```http
GET /concepts/{c_id}/indexes/{index_id}
```

**Response (200):** Single `IndexResponse`.

### Update Index
```http
PATCH /concepts/{c_id}/indexes/{index_id}
```

**Request Body (partial update):**
```json
{
  "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 10000"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New index name |
| `conn_id` | integer | No | New database connection ID |
| `sql_query` | string | No | New SQL query |
| `query_template_params` | object | No | New template parameters |
| `partition_type` | string | No | New partition type |
| `partition_config` | object | No | New partition configuration |

Updating `sql_query` or `query_template_params` resets materialization status.

**Response (200):** Updated `IndexResponse`.

### Delete Index
```http
DELETE /concepts/{c_id}/indexes/{index_id}
```

**Response:** `204 No Content`

### Materialize Index
```http
POST /concepts/{c_id}/indexes/{index_id}/materialize
```

Triggers Dagster to execute the SQL query and store results to S3.

**Response (200):**
```json
{
  "index_id": 1,
  "dagster_run_id": "abc123xyz",
  "status": "STARTED"
}
```

---

## Rules

**Pure feature extraction** layer. Rules compute features from external databases using the index sample set. Rules have no label awareness.

### Create Rule
```http
POST /concepts/{c_id}/rules
```

**Request Body:**
```json
{
  "name": "static_short_irregular_sessions_rule",
  "index_id": 1,
  "sql_query": "WITH connections AS (SELECT mac_address, SUM(CASE WHEN EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS connection_count, COUNT(DISTINCT(space_id)) AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address) SELECT mac_address, connection_count, distinct_ap FROM connections;",
  "index_column": "mac_address"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique rule name within the concept |
| `index_id` | integer | Yes | Index ID to use as sample set |
| `sql_query` | string | Yes | SQL query with `:index_values` placeholder |
| `index_column` | string | No | Column from index to extract for filtering (defaults to first column) |
| `query_template_params` | object | No | Additional Jinja2 template parameters |
| `partition_type` | string | No | Partition type: `time`, `id_range`, `categorical` |
| `partition_config` | object | No | Partition configuration |

**Requirements:**
- The referenced index must belong to the same concept
- The referenced index must be materialized

**Response (201):**
```json
{
  "r_id": 1,
  "c_id": 1,
  "index_id": 1,
  "name": "static_short_irregular_sessions_rule",
  "sql_query": "WITH connections AS (...) SELECT mac_address, connection_count, distinct_ap FROM connections;",
  "index_column": "mac_address",
  "query_template_params": null,
  "partition_type": null,
  "partition_config": null,
  "storage_path": null,
  "is_materialized": false,
  "materialized_at": null,
  "row_count": null,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### List Rules
```http
GET /concepts/{c_id}/rules?skip=0&limit=100
```

**Response (200):** Array of `RuleResponse`.

### Get Rule
```http
GET /concepts/{c_id}/rules/{r_id}
```

**Response (200):** Single `RuleResponse`.

### Update Rule
```http
PATCH /concepts/{c_id}/rules/{r_id}
```

**Request Body (partial update):**
```json
{
  "sql_query": "SELECT mac_address, COUNT(*) as visit_count FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New rule name |
| `index_id` | integer | No | New index ID |
| `sql_query` | string | No | New SQL query |
| `index_column` | string | No | New index column |
| `query_template_params` | object | No | New template parameters |
| `partition_type` | string | No | New partition type |
| `partition_config` | object | No | New partition configuration |

Updating `sql_query` or `query_template_params` resets materialization status.

**Response (200):** Updated `RuleResponse`.

### Delete Rule
```http
DELETE /concepts/{c_id}/rules/{r_id}
```

**Response:** `204 No Content`

### Materialize Rule
```http
POST /concepts/{c_id}/rules/{r_id}/materialize
```

Triggers Dagster to execute the SQL query and store results to S3.

**Requirements:**
- The rule's referenced index must be materialized first

**Response (200):**
```json
{
  "r_id": 1,
  "dagster_run_id": "def456xyz",
  "status": "STARTED"
}
```

**How `:index_values` works:**
1. The index query returns sample records (e.g., 5000 mac_addresses)
2. The system extracts the `index_column` values
3. `:index_values` is replaced with those values in the rule's SQL query
4. The query executes against the external database
5. Results are stored as Parquet in S3

---

## Labeling Functions

Custom Python functions that vote on concept values using rule features. All LFs are custom Python code.

**Key Concepts:**
- Each LF declares `applicable_cv_ids` — the concept values it may vote on
- LF code returns a `cv_id` (e.g., `1` for STATIC) or `-1` to abstain
- All new LFs are created with `is_active=false` and `requires_approval=true`
- If `code` is omitted, an auto-generated template is provided
- LFs support versioning via `parent_lf_id`

### Create Labeling Function
```http
POST /concepts/{c_id}/labeling-functions
```

**Request Body (with code):**
```json
{
  "name": "static_short_irregular_sessions_lf",
  "rule_id": 1,
  "applicable_cv_ids": [1, 2, 3],
  "code": "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:\n        return STATIC\n    return ABSTAIN",
  "allowed_imports": []
}
```

**Request Body (without code — auto-generates template):**
```json
{
  "name": "my_new_lf",
  "rule_id": 1,
  "applicable_cv_ids": [1, 2, 3]
}
```

When `code` is omitted, the system generates a template like:
```python
STATIC = 1
LAPTOP = 2
PHONE = 3
ABSTAIN = -1

def labeling_function(row):
    # TODO: implement voting logic
    # Available columns: row['column_name']
    # Example: return STATIC if condition met
    return ABSTAIN
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique LF name within the concept |
| `rule_id` | integer | Yes | Rule ID that provides the feature DataFrame |
| `applicable_cv_ids` | int[] | Yes | Concept value IDs this LF can vote on |
| `code` | string | No | Python code with `labeling_function(row)`. Auto-generated if omitted |
| `allowed_imports` | string[] | No | Allowed modules: `re`, `datetime`, `math`, `statistics` |
| `parent_lf_id` | integer | No | Parent LF ID for versioning |

**Response (201):**
```json
{
  "lf_id": 1,
  "c_id": 1,
  "applicable_cv_ids": [1, 2, 3],
  "rule_id": 1,
  "name": "static_short_irregular_sessions_lf",
  "version": 1,
  "parent_lf_id": null,
  "lf_type": "custom",
  "lf_config": {
    "code": "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:\n        return STATIC\n    return ABSTAIN",
    "allowed_imports": []
  },
  "is_active": false,
  "requires_approval": true,
  "deprecated_at": null,
  "deprecated_by_lf_id": null,
  "estimated_accuracy": null,
  "coverage": null,
  "conflicts": null,
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### Preview LF Template
```http
GET /concepts/{c_id}/labeling-functions/template?applicable_cv_ids=1&applicable_cv_ids=2&applicable_cv_ids=3
```

Preview the auto-generated template code without creating an LF.

**Response (200):**
```json
{
  "code": "STATIC = 1\nLAPTOP = 2\nPHONE = 3\nABSTAIN = -1\n\ndef labeling_function(row):\n    # TODO: implement voting logic\n    # Available columns: row['column_name']\n    # Example: return STATIC if condition met\n    return ABSTAIN",
  "applicable_cv_ids": [1, 2, 3]
}
```

### List Labeling Functions
```http
GET /concepts/{c_id}/labeling-functions?skip=0&limit=100&active_only=false
```

| Query Parameter | Type | Default | Description |
|-----------------|------|---------|-------------|
| `skip` | integer | 0 | Pagination offset |
| `limit` | integer | 100 | Max results |
| `active_only` | boolean | false | Only return active LFs |

**Response (200):** Array of `LabelingFunctionResponse`.

### Get Labeling Function
```http
GET /concepts/{c_id}/labeling-functions/{lf_id}
```

**Response (200):** Single `LabelingFunctionResponse`.

### Get LF Metrics
```http
GET /concepts/{c_id}/labeling-functions/{lf_id}/metrics
```

Returns performance metrics populated after Snorkel training.

**Response (200):**
```json
{
  "lf_id": 1,
  "name": "static_short_irregular_sessions_lf",
  "version": 1,
  "estimated_accuracy": 0.85,
  "coverage": 0.42,
  "conflicts": 15,
  "metrics_available": true
}
```

### Update Labeling Function
```http
PATCH /concepts/{c_id}/labeling-functions/{lf_id}
```

**Request Body (partial update):**
```json
{
  "name": "renamed_lf",
  "is_active": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | New LF name (must be unique within concept) |
| `is_active` | boolean | No | Active status |
| `lf_config` | object | No | Updated LF configuration |

**Response (200):** Updated `LabelingFunctionResponse`.

### Delete Labeling Function
```http
DELETE /concepts/{c_id}/labeling-functions/{lf_id}
```

**Response:** `204 No Content`

### Approve Labeling Function
```http
POST /concepts/{c_id}/labeling-functions/{lf_id}/approve
```

Approves a labeling function and activates it. Sets `is_active=true` and `requires_approval=false`.

**Response (200):** Updated `LabelingFunctionResponse`.

### Toggle LF Active Status
```http
POST /concepts/{c_id}/labeling-functions/{lf_id}/toggle
```

Toggles `is_active` between `true` and `false`.

**Response (200):** Updated `LabelingFunctionResponse`.

### Create LF Version
```http
POST /concepts/{c_id}/labeling-functions/{lf_id}/versions
```

Creates a new version of an existing LF. The parent LF is deprecated and the new version inherits `rule_id` and `applicable_cv_ids`.

**Request Body:**
```json
{
  "lf_config": {
    "code": "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.9:\n        return STATIC\n    return ABSTAIN",
    "allowed_imports": []
  },
  "name": "static_lf_v2"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `lf_config` | object | Yes | Updated LF configuration |
| `name` | string | No | New name (defaults to `{parent_name}_v{N}`) |

**Response (201):** New `LabelingFunctionResponse` with incremented `version`.

### List LF Versions
```http
GET /concepts/{c_id}/labeling-functions/{lf_id}/versions
```

Returns all versions in the version chain (from root to latest), ordered by version number. You can pass any LF ID in the chain.

**Response (200):** Array of `LabelingFunctionResponse`.

---

## Asset Catalog

Browse available data assets (indexes and rules) for building Snorkel pipelines.

### Get Asset Catalog
```http
GET /concepts/{c_id}/catalog
```

**Response (200):**
```json
{
  "indexes": [
    {
      "id": 1,
      "name": "unique_mac_addresses",
      "is_materialized": true,
      "row_count": 5000,
      "storage_path": "s3://bucket/indexes/index_1/data.parquet"
    }
  ],
  "rules": [
    {
      "id": 1,
      "name": "static_short_irregular_sessions_rule",
      "is_materialized": true,
      "row_count": 4000,
      "storage_path": "s3://bucket/rules/rule_1/data.parquet"
    }
  ]
}
```

### Get Materialized Assets Only
```http
GET /concepts/{c_id}/catalog/materialized
```

Same response format, filtered to only materialized assets.

### Get Catalog Statistics
```http
GET /concepts/{c_id}/catalog/stats
```

**Response (200):**
```json
{
  "concept_id": 1,
  "indexes": {
    "total": 5,
    "materialized": 3,
    "unmaterialized": 2,
    "total_rows": 500000
  },
  "rules": {
    "total": 8,
    "materialized": 6,
    "unmaterialized": 2,
    "total_rows": 45000
  },
  "summary": {
    "total_assets": 13,
    "materialized_assets": 9,
    "total_data_rows": 545000
  }
}
```

---

## Snorkel Training

Trigger and manage Snorkel weak supervision training jobs.

### Run Snorkel Training
```http
POST /concepts/{c_id}/snorkel/run
```

**Request Body:**
```json
{
  "selectedIndex": 1,
  "selectedRules": [1, 2, 3],
  "selectedLFs": [1, 2, 3],
  "snorkel": {
    "epochs": 100,
    "lr": 0.01,
    "sample_size": 10000,
    "output_type": "softmax"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `selectedIndex` | integer | Yes | Index ID to use as dataset |
| `selectedRules` | int[] | No | Rule IDs to include (default: `[]`) |
| `selectedLFs` | int[] | Yes | Labeling function IDs to apply |
| `snorkel.epochs` | integer | No | Training epochs (default: `100`) |
| `snorkel.lr` | float | No | Learning rate (default: `0.01`) |
| `snorkel.sample_size` | integer | No | Sample size for training |
| `snorkel.output_type` | string | No | `softmax` or `hard_labels` (default: `softmax`) |

**Validation:**
- Index must exist and be materialized
- All rules must exist and be materialized
- All labeling functions must exist and be active
- `output_type` must be `softmax` or `hard_labels`

**Response (201):**
```json
{
  "job_id": 1,
  "c_id": 1,
  "index_id": 1,
  "rule_ids": [1, 2, 3],
  "lf_ids": [1, 2, 3],
  "config": {
    "epochs": 100,
    "lr": 0.01,
    "sample_size": 10000
  },
  "output_type": "softmax",
  "dagster_run_id": "abc123xyz",
  "status": "RUNNING",
  "result_path": null,
  "error_message": null,
  "created_at": "2024-01-15T10:30:00Z",
  "completed_at": null
}
```

### List Snorkel Jobs
```http
GET /concepts/{c_id}/snorkel/jobs?skip=0&limit=100
```

Returns jobs ordered by `created_at` descending.

**Response (200):** Array of `SnorkelJobResponse`.

### Get Snorkel Job
```http
GET /concepts/{c_id}/snorkel/jobs/{job_id}
```

**Response (200):** Single `SnorkelJobResponse`.

### Get Snorkel Results
```http
GET /concepts/{c_id}/snorkel/jobs/{job_id}/results
```

**Response (200) — Job not complete:**
```json
{
  "job_id": 1,
  "status": "RUNNING",
  "message": "Job is RUNNING. Results not available yet."
}
```

**Response (200) — Job complete:**
```json
{
  "job_id": 1,
  "status": "SUCCESS",
  "output_type": "softmax",
  "result_path": "s3://bucket/snorkel_results/job_1/results.parquet",
  "message": "Results loading not implemented yet. Check result_path in S3."
}
```

### Delete Snorkel Job
```http
DELETE /concepts/{c_id}/snorkel/jobs/{job_id}
```

**Response:** `204 No Content`

### Cancel Snorkel Job
```http
POST /concepts/{c_id}/snorkel/jobs/{job_id}/cancel
```

Can only cancel jobs with status `PENDING` or `RUNNING`.

**Response (200):** Updated `SnorkelJobResponse` with `status: "CANCELLED"`.

---

## Dagster Integration

Monitor and manage Dagster job execution. All endpoints are prefixed with `/dagster`.

### Get Run Status
```http
GET /dagster/runs/{run_id}
```

**Response (200):**
```json
{
  "run_id": "abc123xyz",
  "status": "SUCCESS",
  "start_time": "2024-01-15T10:30:00Z",
  "end_time": "2024-01-15T10:35:00Z",
  "error_message": null
}
```

Status values: `PENDING`, `STARTED`, `SUCCESS`, `FAILURE`, `CANCELLED`

### List Dagster Runs
```http
GET /dagster/runs?limit=20&status_filter=SUCCESS
```

| Query Parameter | Type | Default | Description |
|-----------------|------|---------|-------------|
| `limit` | integer | 20 | Max runs to return |
| `status_filter` | string | null | Filter by status |

**Response (200):**
```json
{
  "runs": []
}
```

### Cancel Dagster Run
```http
POST /dagster/runs/{run_id}/cancel
```

**Response (200):**
```json
{
  "run_id": "abc123xyz",
  "status": "CANCELLED",
  "message": "Run cancelled successfully"
}
```

### Get Run Logs
```http
GET /dagster/runs/{run_id}/logs?limit=100
```

**Response (200):**
```json
{
  "run_id": "abc123xyz",
  "logs": []
}
```

### List Dagster Assets
```http
GET /dagster/assets
```

**Response (200):**
```json
{
  "assets": []
}
```

### Dagster Health Check
```http
GET /dagster/health
```

**Response (200):**
```json
{
  "status": "healthy",
  "dagster_host": "dagster-webserver",
  "dagster_port": "3000"
}
```

---

## Common Patterns

### Complete Workflow: Concept -> Index -> Rule -> LF -> Snorkel

#### Step 1: Create Database Connection
```http
POST /database-connections

{
  "name": "tippers_db",
  "connection_type": "postgresql",
  "host": "db.example.com",
  "port": 5432,
  "database": "tippers",
  "user": "dbuser",
  "password": "secret"
}
```
Response: `{"conn_id": 1, ...}`

#### Step 2: Create Concept
```http
POST /concepts

{
  "name": "device_classification",
  "description": "Classify devices by usage pattern"
}
```
Response: `{"c_id": 1, ...}`

#### Step 3: Create Concept Values
```http
POST /concepts/1/values
{"name": "STATIC", "description": "Intermittent static device"}
```
Response: `{"cv_id": 1, ...}`

```http
POST /concepts/1/values
{"name": "LAPTOP", "description": "Weekday 9AM-9PM connections"}
```
Response: `{"cv_id": 2, ...}`

```http
POST /concepts/1/values
{"name": "PHONE", "description": "Short daily sessions"}
```
Response: `{"cv_id": 3, ...}`

#### Step 4: Create Index
```http
POST /concepts/1/indexes

{
  "name": "unique_mac_addresses",
  "conn_id": 1,
  "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 5000"
}
```
Response: `{"index_id": 1, ...}`

#### Step 5: Materialize Index
```http
POST /concepts/1/indexes/1/materialize
```
Response: `{"index_id": 1, "dagster_run_id": "abc123", "status": "STARTED"}`

Monitor: `GET /dagster/runs/abc123` — wait for `"status": "SUCCESS"`.

#### Step 6: Create Rules
```http
POST /concepts/1/rules

{
  "name": "static_short_irregular_sessions_rule",
  "index_id": 1,
  "index_column": "mac_address",
  "sql_query": "WITH connections AS (SELECT mac_address, SUM(CASE WHEN EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS connection_count, COUNT(DISTINCT(space_id)) AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address) SELECT mac_address, connection_count, distinct_ap FROM connections;"
}
```
Response: `{"r_id": 1, ...}`

#### Step 7: Materialize Rules
```http
POST /concepts/1/rules/1/materialize
```
Response: `{"r_id": 1, "dagster_run_id": "def456", "status": "STARTED"}`

#### Step 8: Create Labeling Functions
```http
POST /concepts/1/labeling-functions

{
  "name": "static_short_irregular_sessions_lf",
  "rule_id": 1,
  "applicable_cv_ids": [1, 2, 3],
  "code": "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:\n        return STATIC\n    return ABSTAIN",
  "allowed_imports": []
}
```

#### Step 9: Approve Labeling Functions
```http
POST /concepts/1/labeling-functions/1/approve
```

All new LFs require approval before they can be used in Snorkel training.

#### Step 10: Verify Assets
```http
GET /concepts/1/catalog/materialized
```

Confirm all indexes and rules are materialized.

#### Step 11: Run Snorkel Training
```http
POST /concepts/1/snorkel/run

{
  "selectedIndex": 1,
  "selectedRules": [1],
  "selectedLFs": [1],
  "snorkel": {
    "epochs": 100,
    "lr": 0.01,
    "output_type": "softmax"
  }
}
```
Response: `{"job_id": 1, "dagster_run_id": "ghi789", "status": "RUNNING", ...}`

#### Step 12: Monitor and Retrieve Results
```http
GET /concepts/1/snorkel/jobs/1
GET /concepts/1/snorkel/jobs/1/results
```

---

### Quick Reference: Data Flow

```
Index (Sampling)  -->  Rule (Features)  -->  LF (Voting)  -->  Snorkel (Training)
```

**Index** — Defines which records to work with (e.g., 5000 mac_addresses).

**Rule** — Computes features for sampled records. Uses `:index_values` placeholder. Pure feature extraction, no label awareness.

**Labeling Function** — Custom Python code that votes on concept values using rule features. Declares `applicable_cv_ids`. Returns a `cv_id` or `-1` (abstain).

**Snorkel** — Resolves LF disagreements via `LabelModel`. Cardinality is determined from the union of all selected LFs' `applicable_cv_ids`. LF return values (cv_ids) are remapped to 0-indexed class labels internally.

---

## Response Codes

| Code | Description |
|------|-------------|
| `200` | Success |
| `201` | Resource created |
| `204` | Resource deleted (no content) |
| `400` | Invalid request data |
| `404` | Resource not found |
| `500` | Server error |

---

## Interactive Documentation

Once the server is running:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
