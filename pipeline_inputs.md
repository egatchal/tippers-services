# Pipeline Inputs Reference

Based on `rules_and_lfs.yml`. Create these in order: Index → Rules → LFs → Snorkel Run.

**How the pipeline works:**

1. **Index** — defines the entity set (unique MAC addresses via `key_column`)
2. **Rules** — compute metrics/aggregations per entity for ALL entities in the index (no filtering)
3. **LFs** — Python functions that read a rule's metrics and vote on a label; entities not matching get **-1 (ABSTAIN)**
4. **Snorkel** — combines all LF votes into probabilistic labels via a label model, anchored to the full index

> **Incremental execution:** Each stage runs independently. You can materialize the index and rules without any LFs or Snorkel jobs existing. Rules only require a materialized parent index. Snorkel training is only triggered when you explicitly run it.

---

## 1. Index (SQL)

An index is a **set of unique entity identifiers**. The SQL query fetches data, but the system extracts and stores only the unique values from the **Key Column**.

| Field | Value |
|-------|-------|
| Name | `device_trajectory_index` |
| Connection | Select your database connection |
| Key Column | `mac_address` |
| SQL Query | (below) |

```sql
SELECT DISTINCT mac_address
FROM user_location_trajectory
```

> The query returns the unique entity set directly. Rules inherit this key column for their `WHERE mac_address IN (:index_values)` scoping.

---

## 2. Rules (Metric Providers)

Rules compute **metrics per entity** by running SQL against the live database, scoped to the index's entity set. They return metrics for **all** entities (no final filtering). The LF decides what qualifies.

All rules use:
- **Index**: `device_trajectory_index`
- The key column (`mac_address`) is inherited from the index

### Rule 1: `static_short_irregular_sessions_rule`

| Field | Value |
|-------|-------|
| Name | `static_short_irregular_sessions_rule` |
| Index | `device_trajectory_index` |
| SQL Query | (below) |

```sql
WITH connections AS (
    SELECT
        mac_address,
        SUM(CASE WHEN EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS connection_count,
        COUNT(DISTINCT space_id) AS distinct_ap
    FROM user_location_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
)
SELECT mac_address, connection_count, distinct_ap
FROM connections
```

### Rule 2: `laptop_total_connected_duration_rule`

| Field | Value |
|-------|-------|
| Name | `laptop_total_connected_duration_rule` |
| Index | `device_trajectory_index` |
| SQL Query | (below) |

```sql
WITH total_sessions AS (
    SELECT * FROM user_location_trajectory
    WHERE mac_address IN (:index_values)
),
total_connection_count AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM total_sessions
    GROUP BY mac_address
),
valid_connections AS (
    SELECT mac_address
    FROM total_sessions
    WHERE EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5
        AND EXTRACT(HOUR FROM start_time) >= 9
        AND EXTRACT(HOUR FROM end_time) <= 21
),
valid_connection_count AS (
    SELECT mac_address, COUNT(*) AS valid_count
    FROM valid_connections
    GROUP BY mac_address
),
valid_connection_percentage AS (
    SELECT
        t.mac_address,
        v.valid_count * 1.0 / NULLIF(t.total_count, 0) AS percentage
    FROM total_connection_count t
    JOIN valid_connection_count v ON t.mac_address = v.mac_address
)
SELECT mac_address, percentage
FROM valid_connection_percentage
```

### Rule 3: `phone_1_hour_average_rule`

| Field | Value |
|-------|-------|
| Name | `phone_1_hour_average_rule` |
| Index | `device_trajectory_index` |
| SQL Query | (below) |

```sql
WITH total_sessions AS (
    SELECT * FROM user_location_trajectory
    WHERE mac_address IN (:index_values)
),
daily_avg_connection_time AS (
    SELECT
        mac_address,
        DATE(start_time) AS day,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / COUNT(*) / 3600.0 AS avg_hours_per_connection
    FROM total_sessions
    GROUP BY mac_address, DATE(start_time)
),
matched_days AS (
    SELECT mac_address, day
    FROM daily_avg_connection_time
    WHERE avg_hours_per_connection <= 1
),
user_day_counts AS (
    SELECT
        d.mac_address,
        COUNT(DISTINCT d.day) AS total_days,
        COUNT(DISTINCT m.day) AS qualifying_days
    FROM daily_avg_connection_time d
    LEFT JOIN matched_days m ON d.mac_address = m.mac_address AND d.day = m.day
    GROUP BY d.mac_address
),
qualified_users AS (
    SELECT mac_address, total_days
    FROM user_day_counts
    WHERE qualifying_days * 1.0 / total_days >= 0.8
        AND total_days >= 1
)
SELECT mac_address, total_days
FROM qualified_users
```

---

## 3. Labeling Functions (Python Filters)

Each LF receives a **single row** from its rule's output and returns a label or ABSTAIN. The function is named `labeling_function` and receives a pandas row. Concept value constants (`STATIC`, `LAPTOP`, `PHONE`, `ABSTAIN`) are injected automatically.

With the anchor-based label matrix, entities not present in a rule's output automatically get ABSTAIN — the LF is never called for them.

Create one LF per rule. For each:

| Field | Value |
|-------|-------|
| **Name** | `<rule_name>_lf` |
| **Rule** | Select the matching rule |
| **Applicable Labels** | All CVs: STATIC, LAPTOP, PHONE |
| **Python Code** | (below for each) |

---

### `static_short_irregular_sessions_lf` → Rule: `static_short_irregular_sessions_rule` → Label: STATIC

```python
def labeling_function(row):
    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:
        return STATIC
    return ABSTAIN
```

### `laptop_total_connected_duration_lf` → Rule: `laptop_total_connected_duration_rule` → Label: LAPTOP

```python
def labeling_function(row):
    if row['percentage'] >= 0.8:
        return LAPTOP
    return ABSTAIN
```

### `phone_1_hour_average_lf` → Rule: `phone_1_hour_average_rule` → Label: PHONE

```python
def labeling_function(row):
    if row['total_days'] >= 1:
        return PHONE
    return ABSTAIN
```

---

## 4. Snorkel Run

After all LFs are created, trigger a Snorkel run. The label matrix is **anchored to the full index** — every entity gets a row, and uncovered entities get ABSTAIN (-1).

| Field | Value |
|-------|-------|
| **Index** | `device_trajectory_index` |
| **LFs** | Select all 3 LFs |
| **Output Type** | `softmax` (probabilities) or `hard` (single label) |
