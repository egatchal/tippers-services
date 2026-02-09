# Tippers Services - API Reference

Complete CRUD API reference for all endpoints.

## Table of Contents

1. [Database Connections](#database-connections)
2. [Indexes](#indexes)
3. [Rules](#rules)
4. [Labeling Functions](#labeling-functions)
5. [Asset Catalog](#asset-catalog)
6. [Snorkel Training](#snorkel-training)
7. [Dagster Integration](#dagster-integration)

---

## Database Connections

Manage database connections for querying external databases.

### Create Connection
```http
POST /database-connections
```

**Request Body:**
```json
{
  "name": "postgres_main",
  "connection_type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "user": "dbuser",
  "password": "secret123"
}
```

### List Connections
```http
GET /database-connections?skip=0&limit=100
```

### Get Connection
```http
GET /database-connections/{conn_id}
```

### Update Connection
```http
PATCH /database-connections/{conn_id}
```

**Request Body:**
```json
{
  "host": "new-host.example.com",
  "port": 5433
}
```

### Delete Connection
```http
DELETE /database-connections/{conn_id}
```

### Test Connection
```http
POST /database-connections/{conn_id}/test
```

---

## Indexes

Manage index definitions - the **sampling layer** that defines which records to work with.

**Key Concepts:**
- Indexes define the **sample set** of records that all downstream rules will operate on
- Indexes are materialized once and reused by multiple rules
- Indexes connect to external databases to extract sample data
- Rules inherit the database connection from their index

### Create Index
```http
POST /concepts/{c_id}/indexes
```

**Request Body - Simple Example:**
```json
{
  "name": "unique_users_sample",
  "conn_id": 1,
  "sql_query": "SELECT DISTINCT user_id FROM user_activity LIMIT 5000"
}
```

**Request Body - With Template Parameters:**
```json
{
  "name": "user_activity_2024",
  "conn_id": 1,
  "sql_query": "SELECT user_id, activity_date, event_type FROM user_events WHERE year = {{ year }}",
  "query_template_params": {
    "year": 2024
  },
  "partition_type": "time",
  "partition_config": {
    "column": "activity_date",
    "granularity": "monthly"
  }
}
```

**Field Descriptions:**
- `name`: Unique index name within the concept
- `conn_id`: Database connection ID to query
- `sql_query`: SQL SELECT query to extract sample records
- `query_template_params`: Optional Jinja2 template parameters for dynamic queries
- `partition_type`: Optional partitioning strategy (time, id_range, categorical)
- `partition_config`: Configuration for partitioning

**Best Practices:**
- Use `LIMIT` to control sample size
- Select a key column (e.g., `user_id`, `mac_address`) that rules will filter by
- Keep indexes focused on sampling - defer feature computation to rules
- Materialize indexes before creating rules that depend on them

### List Indexes
```http
GET /concepts/{c_id}/indexes?skip=0&limit=100
```

### Get Index
```http
GET /concepts/{c_id}/indexes/{index_id}
```

### Update Index
```http
PATCH /concepts/{c_id}/indexes/{index_id}
```

**Request Body:**
```json
{
  "sql_query": "SELECT user_id, activity_date, event_type, session_id FROM user_events WHERE year = {{ year }}"
}
```

### Delete Index
```http
DELETE /concepts/{c_id}/indexes/{index_id}
```

### Materialize Index
```http
POST /concepts/{c_id}/indexes/{index_id}/materialize
```

Triggers Dagster to execute the SQL query and store results to S3.

---

## Rules

Manage rule/feature definitions (SQL queries for feature engineering on index samples).

**Key Concepts:**
- Rules are **tied to an index** - they inherit the database connection and operate on the index sample
- Rules use the `:index_values` placeholder to filter queries to only the sampled records
- Rules define which labels (concept values) they can identify via `applicable_cv_ids`
- Rules provide guidance for creating labeling functions via `label_guidance`

### Create Rule
```http
POST /concepts/{c_id}/rules
```

**Request Body:**
```json
{
  "name": "user_aggregated_features",
  "index_id": 1,
  "index_column": "mac_address",
  "sql_query": "WITH agg_data AS (SELECT mac_address, AVG(EXTRACT(EPOCH FROM (end_time - start_time))) / 3600 AS avg_hours, COUNT(DISTINCT DATE(start_time)) AS distinct_date, COUNT(DISTINCT space_id) AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address) SELECT * FROM agg_data",
  "applicable_cv_ids": [1, 2],
  "label_guidance": {
    "1": "High engagement: avg_hours > 2 OR distinct_date > 10",
    "2": "Low engagement: avg_hours <= 2 AND distinct_date <= 10"
  },
  "partition_type": null,
  "partition_config": null
}
```

**Field Descriptions:**
- `name`: Unique rule name within the concept
- `index_id`: **Required** - ID of the index to use as sample set
- `index_column`: Column from index to extract for filtering (defaults to first column if not specified)
- `sql_query`: SQL query with `:index_values` placeholder where the index sample will be injected
- `applicable_cv_ids`: Array of concept value IDs this rule can identify
- `label_guidance`: Dictionary mapping cv_id to guidance text for creating labeling functions
- `query_template_params`: Optional additional Jinja2 template parameters
- `partition_type`: Optional partitioning strategy (time, id_range, categorical)
- `partition_config`: Configuration for partitioning

**Example - Simple Rule:**
```json
{
  "name": "user_visit_counts",
  "index_id": 1,
  "index_column": "user_id",
  "sql_query": "SELECT user_id, COUNT(*) as visit_count FROM user_location_trajectory WHERE user_id IN (:index_values) GROUP BY user_id",
  "applicable_cv_ids": [1, 2],
  "label_guidance": {
    "1": "High activity users: visit_count > 50",
    "2": "Low activity users: visit_count <= 50"
  }
}
```

**How `:index_values` Works:**

1. Your index query returns: `SELECT DISTINCT user_id FROM users LIMIT 1000`
2. The system extracts 1000 user_ids: `[123, 456, 789, ...]`
3. `:index_values` gets replaced with: `123, 456, 789, ...`
4. Your rule query becomes: `WHERE user_id IN (123, 456, 789, ...)`

The system automatically:
- Formats values correctly (adds quotes for strings, none for numbers)
- Escapes SQL injection characters
- Handles large lists efficiently

### List Rules
```http
GET /concepts/{c_id}/rules?skip=0&limit=100
```

### Get Rule
```http
GET /concepts/{c_id}/rules/{r_id}
```

**Response:**
```json
{
  "r_id": 1,
  "c_id": 1,
  "index_id": 1,
  "name": "user_aggregated_features",
  "sql_query": "SELECT ...",
  "index_column": "mac_address",
  "query_template_params": null,
  "applicable_cv_ids": [1, 2],
  "label_guidance": {
    "1": "High engagement: avg_hours > 2",
    "2": "Low engagement: avg_hours <= 2"
  },
  "partition_type": null,
  "partition_config": null,
  "storage_path": "s3://bucket/rules/rule_1.parquet",
  "is_materialized": true,
  "materialized_at": "2024-01-15T10:30:00Z",
  "row_count": 4000,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### Update Rule
```http
PATCH /concepts/{c_id}/rules/{r_id}
```

**Request Body (partial update):**
```json
{
  "sql_query": "SELECT user_id, COUNT(*) as visit_count, MAX(visit_date) as last_visit FROM user_activity WHERE user_id IN (:index_values) GROUP BY user_id",
  "label_guidance": {
    "1": "High activity: visit_count > 100",
    "2": "Low activity: visit_count <= 100"
  }
}
```

### Delete Rule
```http
DELETE /concepts/{c_id}/rules/{r_id}
```

### Materialize Rule
```http
POST /concepts/{c_id}/rules/{r_id}/materialize
```

**Requirements:**
- The index specified by `index_id` must be materialized first
- Returns a Dagster run ID to track execution

**Response:**
```json
{
  "r_id": 1,
  "dagster_run_id": "abc123xyz",
  "status": "STARTED"
}
```

**What Happens During Materialization:**
1. Loads the materialized index data (from S3 or local storage)
2. Extracts unique values from the `index_column`
3. Replaces `:index_values` in the SQL query with the extracted values
4. Executes the query on the external database
5. Uploads results to S3 as Parquet file
6. Updates the rule's `is_materialized` status

---

## Labeling Functions

Manage labeling functions for weak supervision.

### Create Template LF
```http
POST /concepts/{c_id}/labeling-functions/template
```

**Request Body:**
```json
{
  "name": "high_activity_label",
  "cv_id": 1,
  "template": "numeric_comparison",
  "field": "event_count",
  "operator": ">",
  "value": 50,
  "label": 1
}
```

### Create Regex LF
```http
POST /concepts/{c_id}/labeling-functions/regex
```

**Request Body:**
```json
{
  "name": "spam_detector",
  "cv_id": 2,
  "pattern": "\\b(viagra|pills|lottery)\\b",
  "field": "message_text",
  "label": 1
}
```

### Create Custom LF
```http
POST /concepts/{c_id}/labeling-functions/custom
```

**Request Body:**
```json
{
  "name": "complex_rule",
  "cv_id": 1,
  "code": "def labeling_function(x):\n    if x.score > 0.7 and x.count > 10:\n        return 1\n    return -1",
  "allowed_imports": ["re", "math"],
  "label": 1
}
```

### List Labeling Functions
```http
GET /concepts/{c_id}/labeling-functions?skip=0&limit=100&active_only=false
```

### Get Labeling Function
```http
GET /concepts/{c_id}/labeling-functions/{lf_id}
```

### Update Labeling Function
```http
PATCH /concepts/{c_id}/labeling-functions/{lf_id}
```

### Delete Labeling Function
```http
DELETE /concepts/{c_id}/labeling-functions/{lf_id}
```

### Approve Custom LF
```http
POST /concepts/{c_id}/labeling-functions/{lf_id}/approve
```

### Toggle LF Active Status
```http
POST /concepts/{c_id}/labeling-functions/{lf_id}/toggle
```

---

## Asset Catalog

Browse available data assets for building Snorkel pipelines.

### Get Asset Catalog
```http
GET /concepts/{c_id}/catalog
```

**Response:**
```json
{
  "indexes": [
    {
      "id": 1,
      "name": "user_activity_2024",
      "is_materialized": true,
      "row_count": 150000,
      "storage_path": "s3://bucket/indexes/index_1/data.parquet"
    }
  ],
  "rules": [
    {
      "id": 1,
      "name": "high_engagement_users",
      "is_materialized": true,
      "row_count": 5000,
      "storage_path": "s3://bucket/rules/rule_1/data.parquet"
    }
  ]
}
```

### Get Materialized Assets Only
```http
GET /concepts/{c_id}/catalog/materialized
```

### Get Catalog Statistics
```http
GET /concepts/{c_id}/catalog/stats
```

**Response:**
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
  "selectedLFs": [1, 2, 3, 4],
  "snorkel": {
    "epochs": 100,
    "lr": 0.01,
    "sample_size": 10000,
    "output_type": "softmax"
  }
}
```

**Response:**
```json
{
  "job_id": 42,
  "c_id": 1,
  "index_id": 1,
  "rule_ids": [1, 2, 3],
  "lf_ids": [1, 2, 3, 4],
  "config": {
    "epochs": 100,
    "lr": 0.01,
    "sample_size": 10000
  },
  "output_type": "softmax",
  "dagster_run_id": "abc123xyz",
  "status": "RUNNING",
  "result_path": null,
  "created_at": "2024-01-15T10:30:00Z",
  "completed_at": null
}
```

### List Snorkel Jobs
```http
GET /concepts/{c_id}/snorkel/jobs?skip=0&limit=100
```

### Get Snorkel Job
```http
GET /concepts/{c_id}/snorkel/jobs/{job_id}
```

### Get Snorkel Results
```http
GET /concepts/{c_id}/snorkel/jobs/{job_id}/results
```

**Response (Softmax):**
```json
{
  "job_id": 42,
  "status": "SUCCESS",
  "output_type": "softmax",
  "result_path": "s3://bucket/snorkel_results/job_42/results.parquet",
  "message": "Results available"
}
```

### Delete Snorkel Job
```http
DELETE /concepts/{c_id}/snorkel/jobs/{job_id}
```

### Cancel Snorkel Job
```http
POST /concepts/{c_id}/snorkel/jobs/{job_id}/cancel
```

---

## Dagster Integration

Monitor and manage Dagster job execution.

### Get Run Status
```http
GET /dagster/runs/{run_id}
```

**Response:**
```json
{
  "run_id": "abc123xyz",
  "status": "SUCCESS",
  "start_time": "2024-01-15T10:30:00Z",
  "end_time": "2024-01-15T10:35:00Z",
  "error_message": null
}
```

### List Dagster Runs
```http
GET /dagster/runs?limit=20&status_filter=SUCCESS
```

### Cancel Dagster Run
```http
POST /dagster/runs/{run_id}/cancel
```

### Get Run Logs
```http
GET /dagster/runs/{run_id}/logs?limit=100
```

### List Dagster Assets
```http
GET /dagster/assets
```

### Dagster Health Check
```http
GET /dagster/health
```

---

## Common Patterns

### Complete Workflow: Index → Rule → Labeling Functions → Snorkel

#### Step 1: Create Database Connection
```http
POST /database-connections
Content-Type: application/json

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
Content-Type: application/json

{
  "name": "user_engagement",
  "description": "Classify users by engagement level"
}
```

Response: `{"c_id": 1, ...}`

#### Step 3: Create Concept Values (Labels)
```http
POST /concepts/1/values
Content-Type: application/json

{
  "name": "HIGH_ENGAGEMENT",
  "description": "High engagement users"
}
```

Response: `{"cv_id": 1, ...}`

```http
POST /concepts/1/values
Content-Type: application/json

{
  "name": "LOW_ENGAGEMENT",
  "description": "Low engagement users"
}
```

Response: `{"cv_id": 2, ...}`

#### Step 4: Create Index (Sample Set)
```http
POST /concepts/1/indexes
Content-Type: application/json

{
  "name": "unique_users_sample",
  "conn_id": 1,
  "sql_query": "SELECT DISTINCT user_id FROM user_activity LIMIT 5000"
}
```

Response: `{"index_id": 1, ...}`

#### Step 5: Materialize Index
```http
POST /concepts/1/indexes/1/materialize
```

Response: `{"index_id": 1, "dagster_run_id": "abc123", "status": "STARTED"}`

Monitor progress:
```http
GET /dagster/runs/abc123
```

Wait until status is `"SUCCESS"`.

#### Step 6: Create Rule (Feature Engineering)
```http
POST /concepts/1/rules
Content-Type: application/json

{
  "name": "user_activity_features",
  "index_id": 1,
  "index_column": "user_id",
  "sql_query": "SELECT user_id, COUNT(*) as visit_count, AVG(session_duration) as avg_duration FROM user_activity WHERE user_id IN (:index_values) GROUP BY user_id",
  "applicable_cv_ids": [1, 2],
  "label_guidance": {
    "1": "High engagement: visit_count > 50 AND avg_duration > 300",
    "2": "Low engagement: visit_count <= 50 OR avg_duration <= 300"
  }
}
```

Response: `{"r_id": 1, ...}`

#### Step 7: Materialize Rule
```http
POST /concepts/1/rules/1/materialize
```

Response: `{"r_id": 1, "dagster_run_id": "def456", "status": "STARTED"}`

#### Step 8: Create Labeling Functions
```http
POST /concepts/1/labeling-functions
Content-Type: application/json

{
  "name": "high_visit_count",
  "rule_id": 1,
  "cv_id": 1,
  "lf_type": "threshold",
  "lf_config": {
    "feature": "visit_count",
    "operator": ">",
    "threshold": 50
  }
}
```

```http
POST /concepts/1/labeling-functions
Content-Type: application/json

{
  "name": "high_avg_duration",
  "rule_id": 1,
  "cv_id": 1,
  "lf_type": "threshold",
  "lf_config": {
    "feature": "avg_duration",
    "operator": ">",
    "threshold": 300
  }
}
```

#### Step 9: Check Catalog
```http
GET /concepts/1/catalog/materialized
```

Verify all assets are materialized before training.

#### Step 10: Run Snorkel Training
```http
POST /concepts/1/snorkel/run
Content-Type: application/json

{
  "selectedIndex": 1,
  "selectedRules": [1],
  "selectedLFs": [1, 2],
  "snorkel": {
    "epochs": 100,
    "lr": 0.01,
    "output_type": "softmax"
  }
}
```

Response: `{"job_id": 42, "dagster_run_id": "ghi789", "status": "RUNNING"}`

#### Step 11: Monitor Job
```http
GET /concepts/1/snorkel/jobs/42
```

#### Step 12: Retrieve Results
```http
GET /concepts/1/snorkel/jobs/42/results
```

Response contains S3 path to labeled data.

---

### Quick Reference: Index → Rule Relationship

**Index (Sampling Layer):**
- Defines **which records** to work with
- Example: "Get 5000 unique users"
- Query: `SELECT DISTINCT user_id FROM users LIMIT 5000`

**Rule (Feature Layer):**
- Computes **features** for the sampled records
- Example: "Calculate visit counts for those 5000 users"
- Query: `SELECT user_id, COUNT(*) FROM activity WHERE user_id IN (:index_values) GROUP BY user_id`
- The `:index_values` placeholder gets replaced with the 5000 user IDs from the index

**Key Points:**
- Rules **must** reference an index via `index_id`
- Rules inherit the database connection from their index
- Rules operate only on the sample defined by the index
- This ensures consistent sampling across all features

---

## Response Codes

- `200 OK` - Success
- `201 Created` - Resource created successfully
- `204 No Content` - Resource deleted successfully
- `400 Bad Request` - Invalid request data
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error

---

## Interactive Documentation

Once the server is running, visit:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

These provide interactive API documentation with the ability to test endpoints directly.
