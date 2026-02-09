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

Manage index definitions (SQL queries that become reusable datasets).

### Create Index
```http
POST /concepts/{c_id}/indexes
```

**Request Body:**
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

Manage rule/feature definitions (SQL queries for feature engineering).

### Create Rule
```http
POST /concepts/{c_id}/rules
```

**Request Body:**
```json
{
  "name": "high_engagement_users",
  "conn_id": 1,
  "sql_query": "SELECT user_id, COUNT(*) as event_count FROM user_events GROUP BY user_id HAVING COUNT(*) > 100",
  "query_template_params": null,
  "partition_type": null,
  "partition_config": null
}
```

### List Rules
```http
GET /concepts/{c_id}/rules?skip=0&limit=100
```

### Get Rule
```http
GET /concepts/{c_id}/rules/{r_id}
```

### Update Rule
```http
PATCH /concepts/{c_id}/rules/{r_id}
```

### Delete Rule
```http
DELETE /concepts/{c_id}/rules/{r_id}
```

### Materialize Rule
```http
POST /concepts/{c_id}/rules/{r_id}/materialize
```

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

### Creating and Materializing an Index

1. Create a database connection:
```http
POST /database-connections
```

2. Create an index:
```http
POST /concepts/1/indexes
```

3. Trigger materialization:
```http
POST /concepts/1/indexes/1/materialize
```

4. Check materialization status:
```http
GET /dagster/runs/{run_id}
```

5. View in catalog:
```http
GET /concepts/1/catalog
```

### Building a Snorkel Pipeline

1. Materialize index and rules (see above)

2. Create labeling functions:
```http
POST /concepts/1/labeling-functions/template
POST /concepts/1/labeling-functions/regex
```

3. Check catalog for materialized assets:
```http
GET /concepts/1/catalog/materialized
```

4. Run Snorkel training:
```http
POST /concepts/1/snorkel/run
```

5. Monitor job progress:
```http
GET /concepts/1/snorkel/jobs/{job_id}
```

6. Retrieve results:
```http
GET /concepts/1/snorkel/jobs/{job_id}/results
```

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
