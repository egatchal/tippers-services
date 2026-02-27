# User Pipeline Inputs — Levels 2-4: Person Classification

Classifies human-owned MAC addresses through three hierarchical Snorkel levels:
- **Level 2**: VISITOR vs RESIDENT (on `people_index`)
- **Level 3**: STUDENT vs STAFF vs FACULTY (on `resident_index`)
- **Level 4**: UNDERGRAD vs GRAD (on `student_index`)

> **Prerequisite**: Complete `device_pipeline_inputs.md` first. The `people_index` (pass-through derived index from Level 1) is the entry point here.

---

## Level 2 — Person Classification

### Parent Index

| Field | Value |
|-------|-------|
| Name | `people_index` |
| Key Column | `mac_address` |

> Created as a derived index in Level 1 with `label_filter=null` (pass-through).

---

### Rules (6 total: 3 VISITOR + 3 RESIDENT)

All rules use **Index**: `people_index`

---

#### VISITOR Rules (3)

##### Rule: `visitor_short_span_rule`

Connection span <= 21 days AND <= 3 unique rooms.

| Field | Value |
|-------|-------|
| Name | `visitor_short_span_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
SELECT
    mac_address,
    (MAX(end_time::date) - MIN(start_time::date)) AS days_span,
    COUNT(DISTINCT space_id) AS unique_rooms
FROM user_ap_trajectory
WHERE mac_address IN (:index_values)
GROUP BY mac_address
```

##### Rule: `visitor_few_days_rule`

Connected on <= 3 unique days.

| Field | Value |
|-------|-------|
| Name | `visitor_few_days_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
SELECT
    mac_address,
    COUNT(DISTINCT start_time::date) AS connected_days
FROM user_ap_trajectory
WHERE mac_address IN (:index_values)
GROUP BY mac_address
```

##### Rule: `visitor_common_rooms_rule`

80%+ in common areas, <= 10% in office/lab.

| Field | Value |
|-------|-------|
| Name | `visitor_common_rooms_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
room_type_counts AS (
    SELECT
        u.mac_address,
        SUM(CASE WHEN sm.type IN ('elevator', 'rest room', 'kitchen', 'conference room', 'meeting room')
            THEN 1 ELSE 0 END) AS common_count,
        SUM(CASE WHEN sm.type IN ('office', 'student lab')
            THEN 1 ELSE 0 END) AS work_count
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE u.mac_address IN (:index_values)
    GROUP BY u.mac_address
)
SELECT t.mac_address,
       r.common_count * 1.0 / NULLIF(t.total_count, 0) AS common_pct,
       r.work_count * 1.0 / NULLIF(t.total_count, 0) AS work_pct
FROM total_counts t
JOIN room_type_counts r ON t.mac_address = r.mac_address
```

---

#### RESIDENT Rules (3)

##### Rule: `resident_long_span_rule`

Connection span > 21 days.

| Field | Value |
|-------|-------|
| Name | `resident_long_span_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
SELECT
    mac_address,
    (MAX(end_time::date) - MIN(start_time::date)) AS days_span
FROM user_ap_trajectory
WHERE mac_address IN (:index_values)
GROUP BY mac_address
```

##### Rule: `resident_many_days_rule`

Connected on 10+ unique days.

| Field | Value |
|-------|-------|
| Name | `resident_many_days_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
SELECT
    mac_address,
    COUNT(DISTINCT start_time::date) AS connected_days
FROM user_ap_trajectory
WHERE mac_address IN (:index_values)
GROUP BY mac_address
```

##### Rule: `resident_work_room_usage_rule`

10%+ connections to office/student lab rooms.

| Field | Value |
|-------|-------|
| Name | `resident_work_room_usage_rule` |
| Index | `people_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
work_counts AS (
    SELECT u.mac_address, COUNT(*) AS work_count
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE u.mac_address IN (:index_values)
        AND sm.type IN ('office', 'student lab')
    GROUP BY u.mac_address
)
SELECT t.mac_address,
       COALESCE(w.work_count, 0) * 1.0 / NULLIF(t.total_count, 0) AS work_pct
FROM total_counts t
LEFT JOIN work_counts w ON t.mac_address = w.mac_address
```

---

### Labeling Functions — Level 2

| Field | Value |
|-------|-------|
| **Applicable Labels** | All CVs: VISITOR, RESIDENT |

#### `visitor_short_span_lf` → Rule: `visitor_short_span_rule` → Label: VISITOR

```python
def labeling_function(row):
    if row['days_span'] <= 21 and row['unique_rooms'] <= 3:
        return VISITOR
    return ABSTAIN
```

#### `visitor_few_days_lf` → Rule: `visitor_few_days_rule` → Label: VISITOR

```python
def labeling_function(row):
    if row['connected_days'] <= 3:
        return VISITOR
    return ABSTAIN
```

#### `visitor_common_rooms_lf` → Rule: `visitor_common_rooms_rule` → Label: VISITOR

```python
def labeling_function(row):
    if row['common_pct'] >= 0.8 and row['work_pct'] <= 0.1:
        return VISITOR
    return ABSTAIN
```

#### `resident_long_span_lf` → Rule: `resident_long_span_rule` → Label: RESIDENT

```python
def labeling_function(row):
    if row['days_span'] > 21:
        return RESIDENT
    return ABSTAIN
```

#### `resident_many_days_lf` → Rule: `resident_many_days_rule` → Label: RESIDENT

```python
def labeling_function(row):
    if row['connected_days'] >= 10:
        return RESIDENT
    return ABSTAIN
```

#### `resident_work_room_usage_lf` → Rule: `resident_work_room_usage_rule` → Label: RESIDENT

```python
def labeling_function(row):
    if row['work_pct'] is not None and row['work_pct'] >= 0.1:
        return RESIDENT
    return ABSTAIN
```

---

### Snorkel Run — Level 2

| Field | Value |
|-------|-------|
| **Index** | `people_index` |
| **LFs** | All 6 Level 2 LFs |
| **Concept Values** | VISITOR, RESIDENT |
| **Cardinality** | 2 |
| **Output Type** | `softmax` or `hard` |

---

### Derived Indexes — Level 2

#### `visitor_index`

| Field | Value |
|-------|-------|
| Name | `visitor_index` |
| Parent Index | `people_index` |
| Snorkel Run | (Level 2 Snorkel run) |
| Label Filter | `VISITOR` |
| Key Column | `mac_address` |

#### `resident_index`

| Field | Value |
|-------|-------|
| Name | `resident_index` |
| Parent Index | `people_index` |
| Snorkel Run | (Level 2 Snorkel run) |
| Label Filter | `RESIDENT` |
| Key Column | `mac_address` |

---

## Level 3 — Resident Classification

### Parent Index

| Field | Value |
|-------|-------|
| Name | `resident_index` |
| Key Column | `mac_address` |

---

### Rules (9 total: 3 STUDENT + 3 STAFF + 3 FACULTY)

All rules use **Index**: `resident_index`

---

#### STUDENT Rules (3)

##### Rule: `student_lab_presence_rule`

50%+ of days have student lab connections (10+ min).

| Field | Value |
|-------|-------|
| Name | `student_lab_presence_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH total_days AS (
    SELECT mac_address, COUNT(DISTINCT start_time::date) AS total_day_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
lab_days AS (
    SELECT u.mac_address, u.start_time::date AS day
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE u.mac_address IN (:index_values) AND sm.type = 'student lab'
    GROUP BY u.mac_address, u.start_time::date
    HAVING SUM(EXTRACT(EPOCH FROM (u.end_time - u.start_time))) / 60.0 >= 10
),
lab_day_counts AS (
    SELECT mac_address, COUNT(*) AS lab_day_count
    FROM lab_days
    GROUP BY mac_address
)
SELECT t.mac_address, t.total_day_count,
       COALESCE(l.lab_day_count, 0) * 1.0 / t.total_day_count AS lab_day_pct
FROM total_days t
LEFT JOIN lab_day_counts l ON t.mac_address = l.mac_address
```

##### Rule: `student_variable_schedule_rule`

Active 3+ days with 5+ distinct 2-hour time bins.

| Field | Value |
|-------|-------|
| Name | `student_variable_schedule_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH daily_times AS (
    SELECT
        mac_address,
        start_time::date AS day,
        FLOOR(EXTRACT(HOUR FROM MIN(start_time)) / 2) AS early_bin,
        FLOOR(EXTRACT(HOUR FROM MAX(end_time)) / 2) AS late_bin
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date
),
bin_counts AS (
    SELECT
        mac_address,
        COUNT(DISTINCT (early_bin::text || '-' || late_bin::text)) AS bin_pair_count,
        COUNT(*) AS day_count
    FROM daily_times
    GROUP BY mac_address
)
SELECT mac_address, day_count, bin_pair_count
FROM bin_counts
```

##### Rule: `student_diverse_rooms_rule`

Avg 6+ distinct rooms daily, no room > 50% of connections.

| Field | Value |
|-------|-------|
| Name | `student_diverse_rooms_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH daily_room_counts AS (
    SELECT
        mac_address, start_time::date AS day, space_id,
        COUNT(*) AS room_connections
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date, space_id
),
daily_totals AS (
    SELECT mac_address, day, SUM(room_connections) AS total_connections
    FROM daily_room_counts
    GROUP BY mac_address, day
),
daily_stats AS (
    SELECT
        r.mac_address, r.day,
        COUNT(DISTINCT r.space_id) AS distinct_rooms,
        MAX(r.room_connections * 1.0 / d.total_connections) AS max_room_ratio
    FROM daily_room_counts r
    JOIN daily_totals d ON r.mac_address = d.mac_address AND r.day = d.day
    GROUP BY r.mac_address, r.day
),
final_stats AS (
    SELECT
        mac_address,
        AVG(distinct_rooms) AS avg_daily_rooms,
        COUNT(*) AS total_days,
        SUM(CASE WHEN max_room_ratio <= 0.5 THEN 1 ELSE 0 END) AS compliant_days
    FROM daily_stats
    GROUP BY mac_address
)
SELECT mac_address, avg_daily_rooms, total_days, compliant_days
FROM final_stats
```

---

#### STAFF Rules (3)

##### Rule: `staff_9to5_schedule_rule`

90%+ of sessions between 9AM-5PM.

| Field | Value |
|-------|-------|
| Name | `staff_9to5_schedule_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
scheduled_counts AS (
    SELECT mac_address, COUNT(*) AS scheduled_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
        AND start_time::time BETWEEN '09:00:00' AND '17:00:00'
        AND end_time::time BETWEEN '09:00:00' AND '17:00:00'
    GROUP BY mac_address
)
SELECT t.mac_address, s.scheduled_count, t.total_count,
       s.scheduled_count * 1.0 / t.total_count AS schedule_pct
FROM total_counts t
JOIN scheduled_counts s ON t.mac_address = s.mac_address
```

##### Rule: `staff_long_office_sessions_rule`

2+ hour sessions in office/meeting rooms on 10+ days.

| Field | Value |
|-------|-------|
| Name | `staff_long_office_sessions_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH long_sessions AS (
    SELECT u.mac_address, u.start_time::date AS day
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON s.building_room = sm.building_room
    WHERE u.mac_address IN (:index_values)
        AND sm.type IN ('office', 'meeting room')
        AND EXTRACT(EPOCH FROM (u.end_time - u.start_time)) / 3600.0 >= 2
),
day_counts AS (
    SELECT mac_address, COUNT(DISTINCT day) AS long_session_days
    FROM long_sessions
    GROUP BY mac_address
)
SELECT mac_address, long_session_days
FROM day_counts
```

##### Rule: `staff_few_rooms_non_faculty_rule`

80% of days in 1-2 rooms (office, not faculty office).

| Field | Value |
|-------|-------|
| Name | `staff_few_rooms_non_faculty_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH rooms_per_day AS (
    SELECT
        mac_address, start_time::date AS day, space_id,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 60.0 AS total_minutes
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date, space_id
    HAVING SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 60.0 >= 10
),
days_1or2_rooms AS (
    SELECT mac_address, day
    FROM rooms_per_day
    GROUP BY mac_address, day
    HAVING COUNT(DISTINCT space_id) <= 2
),
days_with_non_faculty_office AS (
    SELECT r.mac_address, r.day
    FROM rooms_per_day r
    JOIN space s ON r.space_id = s.space_id
    JOIN space_metadata sm ON s.building_room = sm.building_room
    LEFT JOIN faculty_data f ON s.building_room = f.office
    WHERE sm.type = 'office' AND f.office IS NULL
    GROUP BY r.mac_address, r.day
),
both_conditions AS (
    SELECT d.mac_address, d.day
    FROM days_1or2_rooms d
    JOIN days_with_non_faculty_office n ON d.mac_address = n.mac_address AND d.day = n.day
),
total_days AS (
    SELECT mac_address, COUNT(DISTINCT start_time::date) AS total_day_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
ratios AS (
    SELECT t.mac_address,
           COUNT(bc.day) * 1.0 / t.total_day_count AS pct
    FROM total_days t
    LEFT JOIN both_conditions bc ON t.mac_address = bc.mac_address
    GROUP BY t.mac_address, t.total_day_count
)
SELECT mac_address, pct
FROM ratios
```

---

#### FACULTY Rules (3)

##### Rule: `faculty_office_dominance_rule`

80%+ connections in a faculty-designated office.

| Field | Value |
|-------|-------|
| Name | `faculty_office_dominance_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
faculty_room_counts AS (
    SELECT u.mac_address, COUNT(*) AS faculty_count
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN faculty_data f ON s.building_room = f.office
    WHERE u.mac_address IN (:index_values)
    GROUP BY u.mac_address
)
SELECT t.mac_address,
       COALESCE(f.faculty_count, 0) * 1.0 / NULLIF(t.total_count, 0) AS faculty_pct
FROM total_counts t
LEFT JOIN faculty_room_counts f ON t.mac_address = f.mac_address
```

##### Rule: `faculty_long_sessions_rule`

60%+ of total time in long sessions (2+ hr) in office/conference/meeting rooms.

| Field | Value |
|-------|-------|
| Name | `faculty_long_sessions_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH total_duration AS (
    SELECT mac_address,
           SUM(EXTRACT(EPOCH FROM (end_time - start_time))) AS total_seconds
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
long_faculty_seconds AS (
    SELECT u.mac_address,
           SUM(EXTRACT(EPOCH FROM (u.end_time - u.start_time))) AS long_seconds
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE u.mac_address IN (:index_values)
        AND sm.type IN ('office', 'conference room', 'meeting room')
        AND EXTRACT(EPOCH FROM (u.end_time - u.start_time)) >= 7200
    GROUP BY u.mac_address
)
SELECT t.mac_address, t.total_seconds, l.long_seconds,
       l.long_seconds * 1.0 / t.total_seconds AS long_pct
FROM total_duration t
JOIN long_faculty_seconds l ON t.mac_address = l.mac_address
```

##### Rule: `faculty_evening_departure_rule`

80% of days, last connection ends between 5PM-8PM.

| Field | Value |
|-------|-------|
| Name | `faculty_evening_departure_rule` |
| Index | `resident_index` |
| SQL Query | (below) |

```sql
WITH daily_latest AS (
    SELECT
        mac_address,
        start_time::date AS day,
        MAX(end_time::time) AS last_time
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date
),
total_days AS (
    SELECT mac_address, COUNT(*) AS total_day_count
    FROM daily_latest
    GROUP BY mac_address
),
evening_days AS (
    SELECT mac_address, COUNT(*) AS evening_count
    FROM daily_latest
    WHERE last_time BETWEEN '17:00:00' AND '20:00:00'
    GROUP BY mac_address
)
SELECT t.mac_address, t.total_day_count,
       COALESCE(e.evening_count, 0) * 1.0 / t.total_day_count AS evening_pct
FROM total_days t
LEFT JOIN evening_days e ON t.mac_address = e.mac_address
```

---

### Labeling Functions — Level 3

| Field | Value |
|-------|-------|
| **Applicable Labels** | All CVs: STUDENT, STAFF, FACULTY |

#### `student_lab_presence_lf` → Rule: `student_lab_presence_rule` → Label: STUDENT

```python
def labeling_function(row):
    if row['lab_day_pct'] is not None and row['lab_day_pct'] >= 0.5:
        return STUDENT
    return ABSTAIN
```

#### `student_variable_schedule_lf` → Rule: `student_variable_schedule_rule` → Label: STUDENT

```python
def labeling_function(row):
    if row['day_count'] > 3 and row['bin_pair_count'] >= 5:
        return STUDENT
    return ABSTAIN
```

#### `student_diverse_rooms_lf` → Rule: `student_diverse_rooms_rule` → Label: STUDENT

```python
def labeling_function(row):
    if row['avg_daily_rooms'] >= 6 and row['compliant_days'] == row['total_days']:
        return STUDENT
    return ABSTAIN
```

#### `staff_9to5_schedule_lf` → Rule: `staff_9to5_schedule_rule` → Label: STAFF

```python
def labeling_function(row):
    if row['schedule_pct'] >= 0.9:
        return STAFF
    return ABSTAIN
```

#### `staff_long_office_sessions_lf` → Rule: `staff_long_office_sessions_rule` → Label: STAFF

```python
def labeling_function(row):
    if row['long_session_days'] >= 10:
        return STAFF
    return ABSTAIN
```

#### `staff_few_rooms_non_faculty_lf` → Rule: `staff_few_rooms_non_faculty_rule` → Label: STAFF

```python
def labeling_function(row):
    if row['pct'] >= 0.8:
        return STAFF
    return ABSTAIN
```

#### `faculty_office_dominance_lf` → Rule: `faculty_office_dominance_rule` → Label: FACULTY

```python
def labeling_function(row):
    if row['faculty_pct'] is not None and row['faculty_pct'] >= 0.8:
        return FACULTY
    return ABSTAIN
```

#### `faculty_long_sessions_lf` → Rule: `faculty_long_sessions_rule` → Label: FACULTY

```python
def labeling_function(row):
    if row['long_pct'] >= 0.6:
        return FACULTY
    return ABSTAIN
```

#### `faculty_evening_departure_lf` → Rule: `faculty_evening_departure_rule` → Label: FACULTY

```python
def labeling_function(row):
    if row['evening_pct'] is not None and row['evening_pct'] >= 0.8:
        return FACULTY
    return ABSTAIN
```

---

### Snorkel Run — Level 3

| Field | Value |
|-------|-------|
| **Index** | `resident_index` |
| **LFs** | All 9 Level 3 LFs |
| **Concept Values** | STUDENT, STAFF, FACULTY |
| **Cardinality** | 3 |
| **Output Type** | `softmax` or `hard` |

---

### Derived Indexes — Level 3

#### `student_index`

| Field | Value |
|-------|-------|
| Name | `student_index` |
| Parent Index | `resident_index` |
| Snorkel Run | (Level 3 Snorkel run) |
| Label Filter | `STUDENT` |
| Key Column | `mac_address` |

#### `staff_index`

| Field | Value |
|-------|-------|
| Name | `staff_index` |
| Parent Index | `resident_index` |
| Snorkel Run | (Level 3 Snorkel run) |
| Label Filter | `STAFF` |
| Key Column | `mac_address` |

#### `faculty_index`

| Field | Value |
|-------|-------|
| Name | `faculty_index` |
| Parent Index | `resident_index` |
| Snorkel Run | (Level 3 Snorkel run) |
| Label Filter | `FACULTY` |
| Key Column | `mac_address` |

---

## Level 4 — Student Classification

### Parent Index

| Field | Value |
|-------|-------|
| Name | `student_index` |
| Key Column | `mac_address` |

---

### Rules (6 total: 3 UNDERGRAD + 3 GRAD)

All rules use **Index**: `student_index`

---

#### UNDERGRAD Rules (3)

##### Rule: `undergrad_late_nights_rule`

Last connection ends >= 8:30PM on 10+ days.

| Field | Value |
|-------|-------|
| Name | `undergrad_late_nights_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH daily_latest AS (
    SELECT
        mac_address,
        start_time::date AS day,
        MAX(end_time) AS latest_end
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date
),
late_day_counts AS (
    SELECT mac_address, COUNT(*) AS late_days
    FROM daily_latest
    WHERE latest_end::time >= '20:30:00'
    GROUP BY mac_address
)
SELECT mac_address, late_days
FROM late_day_counts
```

##### Rule: `undergrad_shared_rooms_rule`

90%+ of sessions have other devices present.

| Field | Value |
|-------|-------|
| Name | `undergrad_shared_rooms_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH session_occupancy AS (
    SELECT
        u.mac_address,
        CASE WHEN so.space_id IS NOT NULL AND so.number_of_connections >= 1
             THEN 1 ELSE 0 END AS is_shared
    FROM user_ap_trajectory u
    LEFT JOIN space_occupancy so
        ON u.space_id = so.space_id
        AND u.start_time <= so.end_time
        AND so.start_time <= u.end_time
    WHERE u.mac_address IN (:index_values)
),
stats AS (
    SELECT
        mac_address,
        COUNT(*) AS total_count,
        SUM(is_shared) AS shared_count
    FROM session_occupancy
    GROUP BY mac_address
)
SELECT mac_address, total_count, shared_count,
       shared_count * 1.0 / total_count AS shared_pct
FROM stats
```

##### Rule: `undergrad_even_day_spread_rule`

No single day of week has > 40% of connections.

| Field | Value |
|-------|-------|
| Name | `undergrad_even_day_spread_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH dow_counts AS (
    SELECT
        mac_address,
        EXTRACT(DOW FROM start_time) AS dow,
        COUNT(*) AS dow_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, EXTRACT(DOW FROM start_time)
),
total_counts AS (
    SELECT mac_address, SUM(dow_count) AS total_count
    FROM dow_counts
    GROUP BY mac_address
),
max_pct AS (
    SELECT
        d.mac_address,
        MAX(d.dow_count * 1.0 / t.total_count) AS max_day_pct
    FROM dow_counts d
    JOIN total_counts t ON d.mac_address = t.mac_address
    GROUP BY d.mac_address
)
SELECT mac_address, max_day_pct
FROM max_pct
```

---

#### GRAD Rules (3)

##### Rule: `grad_student_lab_weekday_rule`

70%+ of weekday connections in student labs.

| Field | Value |
|-------|-------|
| Name | `grad_student_lab_weekday_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH weekday_sessions AS (
    SELECT * FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
        AND EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5
),
weekday_counts AS (
    SELECT mac_address, COUNT(*) AS total_weekday
    FROM weekday_sessions
    GROUP BY mac_address
),
lab_counts AS (
    SELECT w.mac_address, COUNT(*) AS lab_count
    FROM weekday_sessions w
    JOIN space s ON w.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE sm.type = 'student lab'
    GROUP BY w.mac_address
)
SELECT wc.mac_address,
       COALESCE(lc.lab_count, 0) * 1.0 / wc.total_weekday AS lab_pct
FROM weekday_counts wc
LEFT JOIN lab_counts lc ON wc.mac_address = lc.mac_address
```

##### Rule: `grad_office_and_lab_rule`

80% of days in 3-5 rooms including both office and student lab.

| Field | Value |
|-------|-------|
| Name | `grad_office_and_lab_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH rooms_per_day AS (
    SELECT
        mac_address, start_time::date AS day, space_id,
        SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 60.0 AS total_minutes
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address, start_time::date, space_id
    HAVING SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 60.0 >= 10
),
days_3to5_rooms AS (
    SELECT mac_address, day
    FROM rooms_per_day
    GROUP BY mac_address, day
    HAVING COUNT(DISTINCT space_id) BETWEEN 3 AND 5
),
days_with_both_types AS (
    SELECT r.mac_address, r.day
    FROM rooms_per_day r
    JOIN space s ON r.space_id = s.space_id
    JOIN space_metadata sm ON sm.building_room = s.building_room
    WHERE sm.type IN ('office', 'student lab')
    GROUP BY r.mac_address, r.day
    HAVING COUNT(DISTINCT sm.type) >= 2
),
both_conditions AS (
    SELECT d.mac_address, d.day
    FROM days_3to5_rooms d
    JOIN days_with_both_types b ON d.mac_address = b.mac_address AND d.day = b.day
),
total_days AS (
    SELECT mac_address, COUNT(DISTINCT start_time::date) AS total_day_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
ratios AS (
    SELECT t.mac_address,
           COUNT(bc.day) * 1.0 / t.total_day_count AS pct
    FROM total_days t
    LEFT JOIN both_conditions bc ON t.mac_address = bc.mac_address
    GROUP BY t.mac_address, t.total_day_count
)
SELECT mac_address, pct
FROM ratios
```

##### Rule: `grad_faculty_office_visits_rule`

10%+ connections in faculty offices on 10+ days.

| Field | Value |
|-------|-------|
| Name | `grad_faculty_office_visits_rule` |
| Index | `student_index` |
| SQL Query | (below) |

```sql
WITH total_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
    GROUP BY mac_address
),
faculty_sessions AS (
    SELECT u.mac_address, u.start_time
    FROM user_ap_trajectory u
    JOIN space s ON u.space_id = s.space_id
    JOIN faculty_data f ON s.building_room = f.office
    WHERE u.mac_address IN (:index_values)
        AND EXTRACT(EPOCH FROM (u.end_time - u.start_time)) / 60.0 >= 10
),
faculty_stats AS (
    SELECT mac_address,
           COUNT(*) AS faculty_count,
           COUNT(DISTINCT start_time::date) AS faculty_days
    FROM faculty_sessions
    GROUP BY mac_address
)
SELECT t.mac_address,
       fs.faculty_count * 1.0 / t.total_count AS faculty_pct,
       fs.faculty_days
FROM total_counts t
JOIN faculty_stats fs ON t.mac_address = fs.mac_address
```

---

### Labeling Functions — Level 4

| Field | Value |
|-------|-------|
| **Applicable Labels** | All CVs: UNDERGRAD, GRAD |

#### `undergrad_late_nights_lf` → Rule: `undergrad_late_nights_rule` → Label: UNDERGRAD

```python
def labeling_function(row):
    if row['late_days'] >= 10:
        return UNDERGRAD
    return ABSTAIN
```

#### `undergrad_shared_rooms_lf` → Rule: `undergrad_shared_rooms_rule` → Label: UNDERGRAD

```python
def labeling_function(row):
    if row['shared_pct'] >= 0.9:
        return UNDERGRAD
    return ABSTAIN
```

#### `undergrad_even_day_spread_lf` → Rule: `undergrad_even_day_spread_rule` → Label: UNDERGRAD

```python
def labeling_function(row):
    if row['max_day_pct'] <= 0.4:
        return UNDERGRAD
    return ABSTAIN
```

#### `grad_student_lab_weekday_lf` → Rule: `grad_student_lab_weekday_rule` → Label: GRAD

```python
def labeling_function(row):
    if row['lab_pct'] is not None and row['lab_pct'] >= 0.7:
        return GRAD
    return ABSTAIN
```

#### `grad_office_and_lab_lf` → Rule: `grad_office_and_lab_rule` → Label: GRAD

```python
def labeling_function(row):
    if row['pct'] >= 0.8:
        return GRAD
    return ABSTAIN
```

#### `grad_faculty_office_visits_lf` → Rule: `grad_faculty_office_visits_rule` → Label: GRAD

```python
def labeling_function(row):
    if row['faculty_pct'] >= 0.1 and row['faculty_days'] >= 10:
        return GRAD
    return ABSTAIN
```

---

### Snorkel Run — Level 4

| Field | Value |
|-------|-------|
| **Index** | `student_index` |
| **LFs** | All 6 Level 4 LFs |
| **Concept Values** | UNDERGRAD, GRAD |
| **Cardinality** | 2 |
| **Output Type** | `softmax` or `hard` |
