# Device Pipeline Inputs — Level 1: Device Classification

Classifies MAC addresses into **STATIC**, **LAPTOP**, or **PHONE** via Snorkel, then produces derived indexes including a **PEOPLE** pass-through for downstream user classification.

## CV Tree (Level 1)

```
Devices
├── Static Devices
└── Human Owned Devices
    ├── Laptop Devices
    ├── Cell Phones
    └── People (pass-through, label_filter=null)
```

---

## 1. Index (SQL)

| Field | Value |
|-------|-------|
| Name | `device_index` |
| Connection | Select your database connection |
| Key Column | `mac_address` |
| SQL Query | (below) |

```sql
SELECT DISTINCT mac_address
FROM user_ap_trajectory
```

---

## 2. Rules (Metric Providers)

All rules use:
- **Index**: `device_index`
- Key column `mac_address` is inherited from the index

---

### STATIC Rules (3)

#### Rule: `static_always_on_rule`

Avg 15+ hours connected to a single AP on 10+ days (always-on infrastructure device).

| Field | Value |
|-------|-------|
| Name | `static_always_on_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH agg_data AS (
    SELECT
        mac_address,
        AVG(EXTRACT(EPOCH FROM (end_time - start_time))) / 3600 AS avg_hours,
        COUNT(DISTINCT start_time::date) AS distinct_date,
        COUNT(DISTINCT sensor_id) AS distinct_ap
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
)
SELECT mac_address, avg_hours, distinct_date, distinct_ap
FROM agg_data
```

#### Rule: `static_many_connections_single_ap_rule`

50+ connections to only 1 AP.

| Field | Value |
|-------|-------|
| Name | `static_many_connections_single_ap_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
SELECT
    mac_address,
    COUNT(*) AS record_count,
    COUNT(DISTINCT sensor_id) AS distinct_ap
FROM user_ap_trajectory
WHERE mac_address IN (:index_values)
GROUP BY mac_address
```

#### Rule: `static_off_hour_rule`

80%+ connections during off-hours (10PM-5AM) on 3+ days.

| Field | Value |
|-------|-------|
| Name | `static_off_hour_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
off_hour AS (
    SELECT
        mac_address,
        COUNT(*) AS matched_count,
        COUNT(DISTINCT start_time::date) AS distinct_date
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
        AND (start_time::time >= '22:00:00' OR end_time::time <= '05:00:00')
    GROUP BY mac_address
)
SELECT o.mac_address, o.matched_count, o.distinct_date, t.total_count
FROM off_hour o
JOIN total_counts t ON o.mac_address = t.mac_address
```

---

### LAPTOP Rules (3)

#### Rule: `laptop_daily_avg_rule`

80% of days have avg session 1.5-5 hours.

| Field | Value |
|-------|-------|
| Name | `laptop_daily_avg_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH daily_avg AS (
    SELECT
        mac_address,
        start_time::date AS day,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / COUNT(*) / 3600.0 AS avg_hours
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date
),
day_counts AS (
    SELECT
        mac_address,
        COUNT(*) AS total_days,
        SUM(CASE WHEN avg_hours BETWEEN 1.5 AND 5 THEN 1 ELSE 0 END) AS qualifying_days
    FROM daily_avg
    GROUP BY mac_address
)
SELECT mac_address, total_days, qualifying_days,
       qualifying_days * 1.0 / total_days AS qualifying_ratio
FROM day_counts
```

#### Rule: `laptop_valid_room_type_rule`

80%+ connections in office/conference/meeting/lab rooms.

| Field | Value |
|-------|-------|
| Name | `laptop_valid_room_type_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
valid_counts AS (
    SELECT u.mac_address, COUNT(*) AS valid_count
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE u.mac_address IN (:index_values)
        AND sm.type IN ('conference room', 'office', 'meeting room', 'student lab')
    GROUP BY u.mac_address
)
SELECT t.mac_address,
       COALESCE(v.valid_count, 0) * 1.0 / NULLIF(t.total_count, 0) AS percentage
FROM total_counts t
LEFT JOIN valid_counts v ON t.mac_address = v.mac_address
```

#### Rule: `laptop_weekday_business_hours_rule`

80%+ connections on weekdays 9AM-9PM.

| Field | Value |
|-------|-------|
| Name | `laptop_weekday_business_hours_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
valid_counts AS (
    SELECT mac_address, COUNT(*) AS valid_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
        AND EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5
        AND start_time::time >= '09:00:00'
        AND end_time::time <= '21:00:00'
    GROUP BY mac_address
)
SELECT t.mac_address,
       v.valid_count * 1.0 / NULLIF(t.total_count, 0) AS percentage
FROM total_counts t
JOIN valid_counts v ON t.mac_address = v.mac_address
```

---

### PHONE Rules (3)

#### Rule: `phone_short_sessions_rule`

80% of days have avg session <= 1 hour.

| Field | Value |
|-------|-------|
| Name | `phone_short_sessions_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH daily_avg AS (
    SELECT
        mac_address,
        start_time::date AS day,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / COUNT(*) / 3600.0 AS avg_hours
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date
),
day_counts AS (
    SELECT
        mac_address,
        COUNT(*) AS total_days,
        SUM(CASE WHEN avg_hours <= 1 THEN 1 ELSE 0 END) AS qualifying_days
    FROM daily_avg
    GROUP BY mac_address
)
SELECT mac_address, total_days, qualifying_days,
       qualifying_days * 1.0 / total_days AS qualifying_ratio
FROM day_counts
```

#### Rule: `phone_many_rooms_rule`

6+ rooms per day (2+ min each) on 80% of days, active 3+ days.

| Field | Value |
|-------|-------|
| Name | `phone_many_rooms_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH room_durations AS (
    SELECT
        mac_address, start_time::date AS day, space_id,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 60.0 AS duration_min
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date, space_id
),
total_days AS (
    SELECT mac_address, COUNT(DISTINCT start_time::date) AS total_day_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
days_6plus AS (
    SELECT mac_address, day
    FROM room_durations
    WHERE duration_min >= 2
    GROUP BY mac_address, day
    HAVING COUNT(DISTINCT space_id) >= 6
),
ratios AS (
    SELECT t.mac_address, t.total_day_count,
           COUNT(d.day) * 1.0 / t.total_day_count AS ratio
    FROM total_days t
    LEFT JOIN days_6plus d ON t.mac_address = d.mac_address
    GROUP BY t.mac_address, t.total_day_count
)
SELECT mac_address, total_day_count, ratio
FROM ratios
```

#### Rule: `phone_weekday_daytime_rule`

80%+ connections on weekdays 8AM-10PM.

| Field | Value |
|-------|-------|
| Name | `phone_weekday_daytime_rule` |
| Index | `device_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
valid_counts AS (
    SELECT mac_address, COUNT(*) AS valid_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
        AND EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5
        AND start_time::time >= '08:00:00'
        AND end_time::time <= '22:00:00'
    GROUP BY mac_address
)
SELECT t.mac_address,
       COALESCE(v.valid_count, 0) * 1.0 / NULLIF(t.total_count, 0) AS percentage
FROM total_counts t
LEFT JOIN valid_counts v ON t.mac_address = v.mac_address
```

---

## 3. Labeling Functions (Python Filters)

Each LF receives a single row from its rule's output and returns a label or ABSTAIN.

Create one LF per rule. For each:

| Field | Value |
|-------|-------|
| **Name** | `<rule_name>_lf` |
| **Rule** | Select the matching rule |
| **Applicable Labels** | All CVs: STATIC, LAPTOP, PHONE |
| **Python Code** | (below for each) |

---

### STATIC LFs

#### `static_always_on_lf` → Rule: `static_always_on_rule` → Label: STATIC

```python
def labeling_function(row):
    if row['avg_hours'] >= 15 and row['distinct_date'] >= 10 and row['distinct_ap'] == 1:
        return STATIC
    return ABSTAIN
```

#### `static_many_connections_single_ap_lf` → Rule: `static_many_connections_single_ap_rule` → Label: STATIC

```python
def labeling_function(row):
    if row['record_count'] >= 50 and row['distinct_ap'] == 1:
        return STATIC
    return ABSTAIN
```

#### `static_off_hour_lf` → Rule: `static_off_hour_rule` → Label: STATIC

```python
def labeling_function(row):
    if row['total_count'] > 0 and row['matched_count'] / row['total_count'] >= 0.8 and row['distinct_date'] >= 3:
        return STATIC
    return ABSTAIN
```

---

### LAPTOP LFs

#### `laptop_daily_avg_lf` → Rule: `laptop_daily_avg_rule` → Label: LAPTOP

```python
def labeling_function(row):
    if row['qualifying_ratio'] >= 0.8 and row['total_days'] >= 1:
        return LAPTOP
    return ABSTAIN
```

#### `laptop_valid_room_type_lf` → Rule: `laptop_valid_room_type_rule` → Label: LAPTOP

```python
def labeling_function(row):
    if row['percentage'] is not None and row['percentage'] >= 0.8:
        return LAPTOP
    return ABSTAIN
```

#### `laptop_weekday_business_hours_lf` → Rule: `laptop_weekday_business_hours_rule` → Label: LAPTOP

```python
def labeling_function(row):
    if row['percentage'] is not None and row['percentage'] >= 0.8:
        return LAPTOP
    return ABSTAIN
```

---

### PHONE LFs

#### `phone_short_sessions_lf` → Rule: `phone_short_sessions_rule` → Label: PHONE

```python
def labeling_function(row):
    if row['qualifying_ratio'] >= 0.8 and row['total_days'] >= 1:
        return PHONE
    return ABSTAIN
```

#### `phone_many_rooms_lf` → Rule: `phone_many_rooms_rule` → Label: PHONE

```python
def labeling_function(row):
    if row['ratio'] >= 0.8 and row['total_day_count'] >= 3:
        return PHONE
    return ABSTAIN
```

#### `phone_weekday_daytime_lf` → Rule: `phone_weekday_daytime_rule` → Label: PHONE

```python
def labeling_function(row):
    if row['percentage'] is not None and row['percentage'] >= 0.8:
        return PHONE
    return ABSTAIN
```

---

## 4. Snorkel Run

| Field | Value |
|-------|-------|
| **Index** | `device_index` |
| **LFs** | All 9 LFs above |
| **Concept Values** | STATIC, LAPTOP, PHONE |
| **Cardinality** | 3 |
| **Output Type** | `softmax` (probabilities) or `hard` (single label) |

---

## 5. Derived Indexes

After the Snorkel run completes, create derived indexes from the results. Each derived index filters the parent index by the Snorkel-assigned label.

### `static_device_index` — Static devices

| Field | Value |
|-------|-------|
| Name | `static_device_index` |
| Parent Index | `device_index` |
| Snorkel Run | (the Level 1 Snorkel run above) |
| Label Filter | `STATIC` |
| Key Column | `mac_address` |

### `laptop_device_index` — Laptop devices

| Field | Value |
|-------|-------|
| Name | `laptop_device_index` |
| Parent Index | `device_index` |
| Snorkel Run | (the Level 1 Snorkel run above) |
| Label Filter | `LAPTOP` |
| Key Column | `mac_address` |

### `phone_device_index` — Cell phone devices

| Field | Value |
|-------|-------|
| Name | `phone_device_index` |
| Parent Index | `device_index` |
| Snorkel Run | (the Level 1 Snorkel run above) |
| Label Filter | `PHONE` |
| Key Column | `mac_address` |

### `people_index` — All human-owned devices (pass-through)

| Field | Value |
|-------|-------|
| Name | `people_index` |
| Parent Index | `device_index` |
| Snorkel Run | (the Level 1 Snorkel run above) |
| Label Filter | *(leave empty / null)* |
| Key Column | `mac_address` |

> **Pass-through behavior**: When `label_filter` is empty/null, `resolve_entities.py` returns the full parent dataframe (`df_root`). This means `people_index` contains ALL MAC addresses from `device_index` that were classified as LAPTOP or PHONE (i.e., all non-STATIC human-owned devices). This index feeds Level 2 (Person Classification).
