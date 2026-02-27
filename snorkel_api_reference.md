# Snorkel Pipeline API Reference

> Base URL: `http://localhost:8000` | Interactive docs: `/docs`

All Snorkel pipeline endpoints live under the `/concepts` prefix. Path parameters like `{c_id}` refer to the parent Concept ID.

---

## Table of Contents

1. [Database Connections](#1-database-connections)
2. [Concepts](#2-concepts)
3. [Concept Values (Labels)](#3-concept-values-labels)
4. [Indexes (Data Sources)](#4-indexes-data-sources)
5. [Derived Indexes (Hierarchical)](#5-derived-indexes-hierarchical)
6. [Rules (Feature Engineering)](#6-rules-feature-engineering)
7. [Labeling Functions](#7-labeling-functions)
8. [Snorkel Training](#8-snorkel-training)
9. [Asset Catalog](#9-asset-catalog)
10. [Pipeline Orchestration](#10-pipeline-orchestration)
11. [Data Flow Overview](#11-data-flow-overview)
12. [Schema Reference](#12-schema-reference)

---

## 1. Database Connections

A **Database Connection** stores credentials for an external database that indexes and rules query against. Passwords are encrypted at rest.

### `POST /database-connections`

Create a new database connection.

```json
{
  "name": "tippers_db",
  "connection_type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "tippers",
  "user": "postgres",
  "password": "postgres"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique connection name |
| `connection_type` | string | yes | `postgresql`, `mysql`, etc. |
| `host` | string | yes | Database host |
| `port` | int | yes | Database port |
| `database` | string | yes | Database name |
| `user` | string | yes | Database user |
| `password` | string | yes | Password (encrypted on storage) |

**Response:** `201` — `DatabaseConnectionResponse`

---

### `GET /database-connections`

List all database connections.

| Query Param | Type | Default |
|-------------|------|---------|
| `skip` | int | 0 |
| `limit` | int | 100 |

**Response:** `200` — `DatabaseConnectionResponse[]`

---

### `GET /database-connections/{conn_id}`

Get a single connection by ID.

**Response:** `200` — `DatabaseConnectionResponse`

---

### `POST /database-connections/{conn_id}/test`

Test connectivity to the database.

**Response:** `200` — JSON with success/failure status

---

## 2. Concepts

A **Concept** is the top-level entity for a classification task (e.g. "Devices", "User Role").

### `POST /concepts/`

Create a new concept.

```json
{
  "name": "Devices",
  "description": "Classify WiFi devices by type"
}
```

**Response:** `201` — `ConceptResponse`

---

### `GET /concepts/`

List all concepts.

| Query Param | Type | Default | Description |
|-------------|------|---------|-------------|
| `skip` | int | 0 | Pagination offset |
| `limit` | int | 100 | Max results |

**Response:** `200` — `ConceptResponse[]`

---

### `GET /concepts/{c_id}`

Get a single concept by ID.

**Response:** `200` — `ConceptResponse`

---

### `PATCH /concepts/{c_id}`

Update a concept. Only provided fields are changed.

```json
{
  "description": "Updated description"
}
```

**Response:** `200` — `ConceptResponse`

---

### `DELETE /concepts/{c_id}`

Delete a concept and all its children (values, indexes, rules, LFs, jobs).

**Response:** `204 No Content`

---

## 3. Concept Values (Labels)

**Concept Values** are the class labels for a concept (e.g. "STATIC", "LAPTOP", "PHONE"). They support hierarchical nesting via `parent_cv_id`.

### `POST /concepts/{c_id}/values`

Create a concept value.

```json
{
  "name": "LAPTOP",
  "description": "Weekday 9AM-9PM connections",
  "display_order": 2,
  "level": 1,
  "parent_cv_id": null
}
```

For a child value:

```json
{
  "name": "PERSONAL_LAPTOP",
  "level": 2,
  "parent_cv_id": 2
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Label name |
| `description` | string | no | Description |
| `display_order` | int | no | UI sort order |
| `level` | int | no | Classification depth (default: 1) |
| `parent_cv_id` | int | no | Parent CV ID for hierarchical tree |

If `parent_cv_id` is set, the parent must exist in the same concept.

**Response:** `201` — `ConceptValueResponse`

---

### `GET /concepts/{c_id}/values`

List all concept values (flat list, ordered by `display_order` then `cv_id`).

**Response:** `200` — `ConceptValueResponse[]`

---

### `GET /concepts/{c_id}/values/tree`

Get concept values as a **nested tree**. Root nodes (`parent_cv_id = null`) are top-level; children are nested recursively.

**Response:** `200` — `ConceptValueTreeNode[]`

```json
[
  {
    "cv_id": 1,
    "name": "STATIC",
    "level": 1,
    "parent_cv_id": null,
    "children": []
  },
  {
    "cv_id": 2,
    "name": "LAPTOP",
    "level": 1,
    "parent_cv_id": null,
    "children": [
      {
        "cv_id": 4,
        "name": "PERSONAL_LAPTOP",
        "level": 2,
        "parent_cv_id": 2,
        "children": []
      },
      {
        "cv_id": 5,
        "name": "WORK_LAPTOP",
        "level": 2,
        "parent_cv_id": 2,
        "children": []
      }
    ]
  },
  {
    "cv_id": 3,
    "name": "PHONE",
    "level": 1,
    "parent_cv_id": null,
    "children": []
  }
]
```

---

### `GET /concepts/{c_id}/values/{cv_id}`

Get a single concept value.

**Response:** `200` — `ConceptValueResponse`

---

### `PATCH /concepts/{c_id}/values/{cv_id}`

Update a concept value. Supports re-parenting via `parent_cv_id`.

**Response:** `200` — `ConceptValueResponse`

---

### `DELETE /concepts/{c_id}/values/{cv_id}`

Delete a concept value.

**Response:** `204 No Content`

---

## 4. Indexes (Data Sources)

An **Index** is a SQL query that defines the entity set for classification. The query runs against an external database connection and results are stored as a parquet file on S3.

### `POST /concepts/{c_id}/indexes`

Create a SQL index.

```json
{
  "name": "device_mac_index",
  "conn_id": 1,
  "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 1000",
  "query_template_params": null,
  "partition_type": null,
  "partition_config": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique name within concept |
| `conn_id` | int | yes | Database connection ID |
| `sql_query` | string | yes | SELECT or WITH statement |
| `query_template_params` | object | no | Jinja2 template parameters |
| `partition_type` | string | no | `time`, `id_range`, or `categorical` |
| `partition_config` | object | no | Partition configuration |

**Response:** `201` — `IndexResponse`

---

### `GET /concepts/{c_id}/indexes`

List all indexes (SQL and derived).

| Query Param | Type | Default |
|-------------|------|---------|
| `skip` | int | 0 |
| `limit` | int | 100 |

**Response:** `200` — `IndexResponse[]`

---

### `GET /concepts/{c_id}/indexes/{index_id}`

Get a single index.

**Response:** `200` — `IndexResponse`

---

### `PATCH /concepts/{c_id}/indexes/{index_id}`

Update an index. If `sql_query` or `query_template_params` change, materialization status is reset.

**Response:** `200` — `IndexResponse`

---

### `DELETE /concepts/{c_id}/indexes/{index_id}`

Delete an index.

**Response:** `204 No Content`

---

### `POST /concepts/{c_id}/indexes/{index_id}/materialize`

Trigger Dagster to execute the SQL query, store results as parquet on S3, and update metadata.

**Response:** `200` — `IndexMaterializeResponse`

```json
{
  "index_id": 1,
  "dagster_run_id": "abc-123",
  "status": "LAUNCHED"
}
```

---

### `GET /concepts/{c_id}/indexes/{index_id}/entities`

Preview the resolved entity data from any index type (SQL or derived).

| Query Param | Type | Default | Max |
|-------------|------|---------|-----|
| `limit` | int | 100 | 1000 |
| `offset` | int | 0 | - |

**Response:** `200` — `EntityPreviewResponse`

```json
{
  "index_id": 1,
  "source_type": "sql",
  "total_count": 1000,
  "entities": [
    { "mac_address": "AA:BB:CC:DD:EE:01" },
    { "mac_address": "AA:BB:CC:DD:EE:02" }
  ]
}
```

---

## 5. Derived Indexes (Hierarchical)

A **Derived Index** is a virtual index whose entities come from filtering a parent Snorkel job's predictions. This is how the hierarchical pipeline narrows data at each level.

There are two kinds:

| Kind | Parent | `label_filter` | Use case |
|------|--------|----------------|----------|
| **Root derived** | `parent_index_id` (SQL index) | `null` | Passes all entities through to a new CV node |
| **Child derived** | `parent_snorkel_job_id` (completed job) | `{"labels": {"5": {"min_confidence": 0.8}}}` | Filters entities predicted as a specific class |

### `POST /concepts/{c_id}/indexes/derived`

Create a derived index.

**Root derived** (all entities from SQL index):

```json
{
  "name": "root_all_devices",
  "cv_id": 10,
  "parent_index_id": 1
}
```

**Child derived** (filtered by Snorkel predictions):

```json
{
  "name": "laptop_branch",
  "cv_id": 4,
  "parent_snorkel_job_id": 3,
  "label_filter": {
    "labels": {
      "2": { "min_confidence": 0.8 }
    }
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Unique name within concept |
| `cv_id` | int | yes | Concept value this index belongs to |
| `parent_index_id` | int | one of | Parent SQL index (root derived) |
| `parent_snorkel_job_id` | int | one of | Parent Snorkel job (child derived, must be COMPLETED) |
| `label_filter` | object | no | Confidence thresholds per cv_id |

You must set **exactly one** of `parent_index_id` or `parent_snorkel_job_id`.

**Validation:**
- `cv_id` must belong to the same concept
- `parent_snorkel_job_id` must reference a job with `status = "COMPLETED"`
- Setting both `parent_index_id` and `parent_snorkel_job_id` is rejected

The system automatically:
- Inherits `conn_id` by tracing up to the root SQL index
- Computes `filtered_count` via recursive entity resolution
- Sets `is_materialized = true` (derived indexes are virtual)

**Response:** `201` — `IndexResponse`

---

### `PATCH /concepts/{c_id}/indexes/{index_id}/filter`

Update the label filter on a derived index. Recomputes `filtered_count`.

Only valid for derived indexes with a `parent_snorkel_job_id`. Rejected on SQL indexes and root derived indexes.

```json
{
  "label_filter": {
    "labels": {
      "2": { "min_confidence": 0.9 },
      "3": { "min_confidence": 0.7 }
    }
  }
}
```

**Response:** `200` — `IndexResponse`

---

### Label Filter Format

```json
{
  "labels": {
    "<cv_id>": { "min_confidence": <float 0.0-1.0> },
    "<cv_id>": {}
  }
}
```

- Keys are stringified `cv_id` values from the parent Snorkel job's `cv_id_to_index` mapping
- `min_confidence` is the minimum softmax probability threshold
- Empty object `{}` = include all entities predicted as that class (no threshold)
- Multiple labels = union (entity matches if it passes ANY label's filter)

---

## 6. Rules (Feature Engineering)

A **Rule** is a SQL query that computes features for a set of entities. The query uses the `:index_values` placeholder which gets replaced with a subquery against a temp table of entity IDs (avoids SQL string length limits for large entity sets).

**Important:** The parent index must be materialized before rules can be created.

### `POST /concepts/{c_id}/rules`

Create a rule.

```json
{
  "name": "static_short_irregular_sessions_rule",
  "index_id": 1,
  "sql_query": "WITH connections AS (SELECT mac_address, COUNT(*) AS cnt FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address) SELECT mac_address, cnt FROM connections",
  "index_column": "mac_address"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Rule name |
| `index_id` | int | yes | Parent index (must be materialized) |
| `sql_query` | string | yes | SQL with `:index_values` placeholder |
| `index_column` | string | no | Column from index to use as entity ID (defaults to first column) |
| `query_template_params` | object | no | Jinja2 template parameters |
| `partition_type` | string | no | Partition strategy |
| `partition_config` | object | no | Partition config |

**Validation:**
- Parent index must exist and belong to the same concept
- Parent index must be materialized (`is_materialized = true`)
- SQL must start with `SELECT` or `WITH`
- Name must be unique within concept

**Response:** `201` — `RuleResponse`

---

### `GET /concepts/{c_id}/rules`

List all rules.

**Response:** `200` — `RuleResponse[]`

---

### `GET /concepts/{c_id}/rules/{r_id}`

Get a single rule.

**Response:** `200` — `RuleResponse`

---

### `PATCH /concepts/{c_id}/rules/{r_id}`

Update a rule.

**Response:** `200` — `RuleResponse`

---

### `DELETE /concepts/{c_id}/rules/{r_id}`

Delete a rule.

**Response:** `204 No Content`

---

### `POST /concepts/{c_id}/rules/{r_id}/materialize`

Trigger Dagster to execute the rule SQL against the external database. Entity IDs are batch-inserted into a temp table `_tippers_entity_ids` and `:index_values` is replaced with `(SELECT entity_id FROM _tippers_entity_ids)`.

**Prerequisite:** The parent index must be materialized.

**Response:** `200` — `RuleMaterializeResponse`

```json
{
  "r_id": 1,
  "dagster_run_id": "def-456",
  "status": "LAUNCHED"
}
```

---

## 7. Labeling Functions

A **Labeling Function (LF)** is a voting rule that applies to a rule's computed features and emits a class label (or abstains). LFs are versioned and use custom Python code.

### `POST /concepts/{c_id}/labeling-functions`

Create a labeling function.

```json
{
  "name": "static_short_irregular_sessions_lf",
  "rule_id": 1,
  "applicable_cv_ids": [1, 2, 3],
  "code": "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:\n        return STATIC\n    return ABSTAIN\n",
  "allowed_imports": [],
  "parent_lf_id": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | LF name |
| `rule_id` | int | yes | Rule that provides feature columns |
| `applicable_cv_ids` | int[] | yes | CV IDs this LF can vote on |
| `code` | string | no | Python code (auto-generated template if omitted) |
| `allowed_imports` | string[] | no | Allowed Python imports |
| `parent_lf_id` | int | no | Parent LF ID for version chains |

**Notes:**
- The LF is created with `is_active = false` and `requires_approval = true`
- `lf_type` is set to `custom`
- The `code` field should define a function called `labeling_function(row)` that returns a cv_id or `ABSTAIN = -1`
- Snorkel injects correct cv_id constants at runtime by looking up CV names in the DB

**Response:** `201` — `LabelingFunctionResponse`

---

### `GET /concepts/{c_id}/labeling-functions`

List all labeling functions.

| Query Param | Type | Default | Description |
|-------------|------|---------|-------------|
| `skip` | int | 0 | Pagination offset |
| `limit` | int | 100 | Max results |
| `active_only` | bool | false | Filter to active LFs only |

**Response:** `200` — `LabelingFunctionResponse[]`

---

### `GET /concepts/{c_id}/labeling-functions/template`

Preview an auto-generated LF template.

| Query Param | Type | Required | Description |
|-------------|------|----------|-------------|
| `applicable_cv_ids` | int[] | yes | CV IDs to generate constants for |

**Response:** `200` — JSON with generated code template

---

### `GET /concepts/{c_id}/labeling-functions/{lf_id}`

Get a single labeling function.

**Response:** `200` — `LabelingFunctionResponse`

---

### `GET /concepts/{c_id}/labeling-functions/{lf_id}/metrics`

Get performance metrics (accuracy, coverage, conflicts) from the most recent Snorkel run.

**Response:** `200` — JSON with metrics

---

### `PATCH /concepts/{c_id}/labeling-functions/{lf_id}`

Update a labeling function.

**Response:** `200` — `LabelingFunctionResponse`

---

### `DELETE /concepts/{c_id}/labeling-functions/{lf_id}`

Delete a labeling function.

**Response:** `204 No Content`

---

### `POST /concepts/{c_id}/labeling-functions/{lf_id}/versions`

Create a new version of a labeling function.

```json
{
  "lf_config": { "code": "def labeling_function(row): ..." },
  "name": "static_lf_v2"
}
```

**Response:** `201` — `LabelingFunctionResponse`

---

### `GET /concepts/{c_id}/labeling-functions/{lf_id}/versions`

List all versions of a labeling function.

**Response:** `200` — `LabelingFunctionResponse[]`

---

### `POST /concepts/{c_id}/labeling-functions/{lf_id}/approve`

Approve and activate a labeling function. Sets `requires_approval = false` and `is_active = true`.

**Response:** `200` — `LabelingFunctionResponse`

---

### `POST /concepts/{c_id}/labeling-functions/{lf_id}/toggle`

Toggle active/inactive status.

**Response:** `200` — `LabelingFunctionResponse`

---

## 8. Snorkel Training

### `POST /concepts/{c_id}/snorkel/run`

Trigger Snorkel label model training.

**Prerequisites (all validated before job creation):**
- Index must be materialized
- All referenced rules must be materialized
- All referenced LFs must be active (`is_active = true`, `requires_approval = false`)

```json
{
  "selectedIndex": 1,
  "selectedRules": [1, 2, 3],
  "selectedLFs": [1, 2, 3],
  "snorkel": {
    "epochs": 100,
    "lr": 0.01,
    "sample_size": null,
    "output_type": "softmax"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `selectedIndex` | int | yes | Materialized index ID |
| `selectedRules` | int[] | no | Rule IDs (features, all must be materialized) |
| `selectedLFs` | int[] | yes | Active LF IDs |
| `snorkel.epochs` | int | no | Training epochs (default: 100) |
| `snorkel.lr` | float | no | Learning rate (default: 0.01) |
| `snorkel.sample_size` | int | no | Subsample size |
| `snorkel.output_type` | string | no | `softmax` or `hard_labels` (default: `softmax`) |

**Response:** `201` — `SnorkelJobResponse`

---

### `GET /concepts/{c_id}/snorkel/jobs`

List all Snorkel training jobs (newest first).

| Query Param | Type | Default |
|-------------|------|---------|
| `skip` | int | 0 |
| `limit` | int | 100 |

**Response:** `200` — `SnorkelJobResponse[]`

---

### `GET /concepts/{c_id}/snorkel/jobs/{job_id}`

Get a single Snorkel job.

**Response:** `200` — `SnorkelJobResponse`

---

### `GET /concepts/{c_id}/snorkel/jobs/{job_id}/results`

Get training results. Metadata is read directly from PostgreSQL JSONB columns (fast, no S3 download). Per-sample predictions are optionally loaded from the S3 parquet.

Falls back to legacy JSON download for pre-migration jobs (where `lf_summary` is NULL and `result_path` ends in `.json`).

| Query Param | Type | Default | Description |
|-------------|------|---------|-------------|
| `include_predictions` | bool | false | Also download predictions parquet from S3 |

**Response (COMPLETED job):**

```json
{
  "job_id": 3,
  "status": "COMPLETED",
  "output_type": "softmax",
  "lf_summary": [
    {
      "lf_id": 1,
      "name": "static_lf",
      "polarity": [0, 1],
      "coverage": 0.45,
      "overlaps": 0.32,
      "conflicts": 0.05,
      "n_votes": 2450,
      "learned_weight": 1.23
    }
  ],
  "label_matrix_class_distribution": {
    "STATIC": 312,
    "LAPTOP": 245,
    "PHONE": 443
  },
  "model_class_distribution": {
    "STATIC": 350,
    "LAPTOP": 260,
    "PHONE": 390
  },
  "overall_stats": {
    "n_samples": 1000,
    "n_lfs": 3,
    "cardinality": 3,
    "total_coverage": 0.79,
    "mean_lf_coverage": 0.48,
    "mean_lf_overlaps": 0.30,
    "mean_lf_conflicts": 0.04
  },
  "cv_id_to_name": { "1": "STATIC", "2": "LAPTOP", "3": "PHONE" },
  "cv_id_to_index": { "1": 0, "2": 1, "3": 2 },
  "predictions": null
}
```

**JSONB storage format** (in `snorkel_jobs.class_distribution` column):

```json
{
  "label_matrix": { "STATIC": 312, "LAPTOP": 245, "PHONE": 443 },
  "model": { "STATIC": 350, "LAPTOP": 260, "PHONE": 390 }
}
```

The results endpoint unpacks this into `label_matrix_class_distribution` and `model_class_distribution`.

When `include_predictions=true`, the `predictions` field contains:

```json
{
  "predictions": {
    "sample_ids": [0, 1, 2, ...],
    "probabilities": [[0.85, 0.10, 0.05], [0.2, 0.7, 0.1], ...]
  }
}
```

**Response (non-COMPLETED job):**

```json
{
  "job_id": 4,
  "status": "RUNNING",
  "message": "Job is RUNNING. Results not available yet."
}
```

---

### `DELETE /concepts/{c_id}/snorkel/jobs/{job_id}`

Delete a Snorkel job.

**Response:** `204 No Content`

---

### `POST /concepts/{c_id}/snorkel/jobs/{job_id}/cancel`

Cancel a running or pending Snorkel job.

**Response:** `200` — `SnorkelJobResponse`

---

## 9. Asset Catalog

Summary views of all assets for a concept.

### `GET /concepts/{c_id}/catalog`

Get all assets (indexes and rules) for a concept.

**Response:** `200` — `AssetCatalogResponse`

```json
{
  "indexes": [
    { "id": 1, "name": "device_macs", "is_materialized": true, "row_count": 1000, "storage_path": "indexes/index_1.parquet" }
  ],
  "rules": [
    { "id": 1, "name": "static_rule", "is_materialized": true, "row_count": 950, "storage_path": "rules/rule_1.parquet" }
  ]
}
```

---

### `GET /concepts/{c_id}/catalog/materialized`

Get only materialized assets. Useful for the pipeline builder UI.

**Response:** `200` — `AssetCatalogResponse`

---

### `GET /concepts/{c_id}/catalog/stats`

Get aggregate statistics for a concept's assets.

**Response:** `200`

```json
{
  "concept_id": 1,
  "indexes": { "total": 3, "materialized": 2, "unmaterialized": 1, "total_rows": 2500 },
  "rules": { "total": 5, "materialized": 3, "unmaterialized": 2, "total_rows": 4800 },
  "summary": { "total_assets": 8, "materialized_assets": 5, "total_data_rows": 7300 }
}
```

---

## 10. Pipeline Orchestration

Endpoints for dependency-aware staleness detection, cascade invalidation, and full pipeline execution.

### `GET /concepts/{c_id}/pipeline/staleness`

Compute staleness status for every asset in the concept's pipeline.

**Response:**

```json
{
  "indexes": [
    {
      "index_id": 1,
      "name": "device_macs",
      "source_type": "sql",
      "is_materialized": true,
      "stale": false,
      "reason": null
    },
    {
      "index_id": 4,
      "name": "laptop_branch",
      "source_type": "derived",
      "is_materialized": true,
      "stale": true,
      "reason": "Parent Snorkel job re-completed"
    }
  ],
  "rules": [
    {
      "r_id": 1,
      "name": "static_rule",
      "index_id": 1,
      "is_materialized": true,
      "stale": true,
      "reason": "Parent index re-materialized"
    }
  ],
  "snorkel_jobs": [
    {
      "job_id": 3,
      "index_id": 1,
      "status": "COMPLETED",
      "stale": true,
      "reason": "Rule 1 re-materialized after job creation"
    }
  ]
}
```

**Staleness rules:**

| Asset Type | Stale When |
|------------|------------|
| SQL Index | Not materialized, or `updated_at > materialized_at` (definition changed) |
| Rule | Not materialized, or parent index `materialized_at > rule.materialized_at` |
| Snorkel Job | Any input rule or index re-materialized after `job.created_at` |
| Derived Index | Parent Snorkel job `completed_at > derived.materialized_at` |

---

### `POST /concepts/{c_id}/pipeline/invalidate/{index_id}`

Walk the dependency tree from an index and mark all downstream assets as unmaterialized.

**Cascade order:** Index → Rules → Snorkel Jobs → Derived Indexes

**Response:**

```json
{
  "index_id": 1,
  "invalidated_count": 4,
  "invalidated": [
    { "type": "rule", "id": 1 },
    { "type": "snorkel_job", "id": 3, "note": "stale" },
    { "type": "derived_index", "id": 4 },
    { "type": "rule", "id": 2 }
  ]
}
```

---

### `POST /concepts/{c_id}/pipeline/execute`

Trigger full pipeline execution. Submits Dagster jobs for stale assets in dependency order.

| Query Param | Type | Required | Description |
|-------------|------|----------|-------------|
| `index_id` | int | no | Specific SQL index to run pipeline for. If omitted, all stale assets are re-materialized. |

**Response:**

```json
{
  "concept_id": 1,
  "triggered_runs": [
    { "step": "materialize_index", "index_id": 1, "dagster_run_id": "abc-123" },
    { "step": "materialize_rule", "rule_id": 1, "dagster_run_id": "def-456" },
    { "step": "materialize_rule", "rule_id": 2, "dagster_run_id": "ghi-789" }
  ],
  "total_triggered": 3
}
```

---

## 11. Data Flow Overview

### Flat Pipeline (Single Level)

```
SQL Index ──materialize──> parquet on S3
     │
     ├── Rule 1 ──materialize──> parquet on S3
     ├── Rule 2 ──materialize──> parquet on S3
     │
     └── Snorkel Training (LF 1, LF 2, LF 3)
              │
              ├── predictions.parquet on S3
              └── metadata (JSONB) in PostgreSQL
```

### Hierarchical Pipeline (Multi-Level)

```
SQL Index (parquet on S3)
     │
Root Derived Index (label_filter=null, all entities)
  ├── Rule A ──> LF 1, LF 2 ──> Snorkel Job #1 (Level 1)
  │                                    │
  │             ┌──────────────────────┼──────────────────────┐
  │    STATIC Derived             LAPTOP Derived           PHONE Derived
  │    (cv 1, P>=0.8)            (cv 2, P>=0.8)           (cv 3, no threshold)
  │                                    │
  │                          Rule B ──> LF 3 ──> Job #2 (Level 2)
  │                                         │
  │                                ┌────────┼────────┐
  │                         PERSONAL Derived    WORK Derived
  │                         (cv 4, P>=0.7)      (cv 5, P>=0.7)
  │                                │
  │                          Rule C ──> LF 4 ──> Job #3 (Level 3)
```

Each level narrows the entity set by filtering on the previous Snorkel job's predicted probabilities.

### Ordering Constraint

Level N+1 **cannot** begin until Level N's Snorkel job has status `COMPLETED`. Specifically:

1. **SQL Index** must be materialized before rules can be created
2. **Rules** must be materialized before Snorkel training can run
3. **LFs** must be approved (`is_active = true`) before Snorkel training
4. **Snorkel job** must be `COMPLETED` before child derived indexes can reference it

### Storage Model

| Artifact | Location | Lifecycle |
|----------|----------|-----------|
| Index parquet | S3 `indexes/index_{id}.parquet` | Overwritten on re-materialize |
| Rule parquet | S3 `rules/rule_{id}.parquet` | Overwritten on re-materialize |
| Predictions parquet | S3 `snorkel_jobs/job_{id}.parquet` | Immutable per job |
| LF summary, class distribution, stats | PostgreSQL JSONB on `snorkel_jobs` row | Immutable per job |
| CV-to-index/name mappings | PostgreSQL JSONB on `snorkel_jobs` row | Immutable per job |

---

## 12. Schema Reference

### DatabaseConnectionResponse

| Field | Type | Description |
|-------|------|-------------|
| `conn_id` | int | Connection ID |
| `name` | string | Connection name |
| `connection_type` | string | Database type |
| `host` | string | Database host |
| `port` | int | Database port |
| `database` | string | Database name |
| `user` | string | Database user |
| `created_at` | datetime | Creation timestamp |

### ConceptResponse

| Field | Type | Description |
|-------|------|-------------|
| `c_id` | int | Concept ID |
| `name` | string | Concept name |
| `description` | string? | Description |
| `created_at` | datetime | Creation timestamp |
| `updated_at` | datetime | Last update timestamp |

### ConceptValueResponse / ConceptValueTreeNode

| Field | Type | Description |
|-------|------|-------------|
| `cv_id` | int | Concept value ID |
| `c_id` | int | Parent concept ID |
| `name` | string | Label name |
| `description` | string? | Description |
| `display_order` | int? | UI sort order |
| `level` | int | Hierarchy depth (1 = top) |
| `parent_cv_id` | int? | Parent CV for tree nesting |
| `created_at` | datetime | Creation timestamp |
| `children` | TreeNode[] | *(TreeNode only)* Nested children |

### IndexResponse

| Field | Type | Description |
|-------|------|-------------|
| `index_id` | int | Index ID |
| `c_id` | int | Parent concept ID |
| `conn_id` | int? | Database connection ID |
| `name` | string | Index name |
| `sql_query` | string? | SQL query (null for derived) |
| `source_type` | string | `sql` or `derived` |
| `cv_id` | int? | Concept value (derived only) |
| `parent_index_id` | int? | Parent SQL index (root derived) |
| `parent_snorkel_job_id` | int? | Parent Snorkel job (child derived) |
| `label_filter` | object? | Confidence thresholds per cv_id |
| `filtered_count` | int? | Entity count after filtering |
| `is_materialized` | bool | Whether data is ready |
| `materialized_at` | datetime? | Last materialization time |
| `row_count` | int? | Number of rows |
| `column_stats` | object? | Per-column statistics |
| `storage_path` | string? | S3 or local parquet path |
| `created_at` | datetime | Creation timestamp |
| `updated_at` | datetime | Last update timestamp |

### RuleResponse

| Field | Type | Description |
|-------|------|-------------|
| `r_id` | int | Rule ID |
| `c_id` | int | Parent concept ID |
| `index_id` | int | Parent index ID |
| `name` | string | Rule name |
| `sql_query` | string | SQL with `:index_values` placeholder |
| `index_column` | string? | Entity ID column from index |
| `query_template_params` | object? | Jinja2 template parameters |
| `partition_type` | string? | Partition strategy |
| `partition_config` | object? | Partition config |
| `storage_path` | string? | S3 parquet path |
| `is_materialized` | bool | Whether features are computed |
| `materialized_at` | datetime? | Last materialization time |
| `row_count` | int? | Number of feature rows |
| `column_stats` | object? | Per-column statistics |
| `created_at` | datetime | Creation timestamp |
| `updated_at` | datetime | Last update timestamp |

### LabelingFunctionResponse

| Field | Type | Description |
|-------|------|-------------|
| `lf_id` | int | LF ID |
| `c_id` | int | Parent concept ID |
| `rule_id` | int | Rule that provides features |
| `applicable_cv_ids` | int[] | CV IDs this LF can vote on |
| `name` | string | LF name |
| `version` | int | Version number |
| `parent_lf_id` | int? | Previous version's LF ID |
| `lf_type` | string | `custom` |
| `lf_config` | object | `{"code": "...", "allowed_imports": [...]}` |
| `is_active` | bool | Whether LF is active (needs approval first) |
| `requires_approval` | bool | Whether LF needs approval before activation |
| `deprecated_at` | datetime? | Deprecation timestamp |
| `deprecated_by_lf_id` | int? | Replacement LF ID |
| `estimated_accuracy` | float? | Learned weight from Snorkel |
| `coverage` | float? | Fraction of samples with votes |
| `conflicts` | int? | Number of conflicting votes |
| `created_at` | datetime | Creation timestamp |
| `updated_at` | datetime | Last update timestamp |

### SnorkelJobResponse

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | int | Job ID |
| `c_id` | int | Parent concept ID |
| `index_id` | int | Input index ID |
| `rule_ids` | int[]? | Rule IDs used |
| `lf_ids` | int[]? | LF IDs applied |
| `config` | object? | Training config (epochs, lr) |
| `output_type` | string | `softmax` or `hard_labels` |
| `dagster_run_id` | string? | Dagster run identifier |
| `status` | string | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `result_path` | string? | S3 path to predictions parquet |
| `error_message` | string? | Error details if failed |
| `created_at` | datetime | Job creation time |
| `completed_at` | datetime? | Completion time |

### SnorkelJob JSONB Metadata (DB columns, not in API response schema)

These columns are populated on the `snorkel_jobs` table when training completes. They are surfaced through the results endpoint.

| Column | Type | Description |
|--------|------|-------------|
| `lf_summary` | JSONB | Per-LF stats (coverage, accuracy, weights, etc.) |
| `class_distribution` | JSONB | `{"label_matrix": {...}, "model": {...}}` — counts per class |
| `overall_stats` | JSONB | Aggregate stats (n_samples, cardinality, coverage, etc.) |
| `cv_id_to_index` | JSONB | Maps stringified cv_id → array index in softmax output |
| `cv_id_to_name` | JSONB | Maps stringified cv_id → human-readable class name |

### EntityPreviewResponse

| Field | Type | Description |
|-------|------|-------------|
| `index_id` | int | Index ID |
| `source_type` | string | `sql` or `derived` |
| `total_count` | int | Total entity count |
| `entities` | object[] | Paginated entity rows |
