# Group Queries — PostgreSQL (Feature Extraction, No Snorkel)

These 3 rules discover `space_name` affiliations via `region_to_coverage`. The output is the **dominant group per entity** (a space name), not a fixed concept value. No Snorkel run is needed — these are rules-only feature extraction queries.

> **Join pattern**: These queries use `sensor_id` → `region_to_coverage.sensor` (not `space_id`), because group affiliation is determined by AP coverage regions, not room-level space IDs.

---

## Parent Index

These rules can run on any device/user index that contains `mac_address` entities. Typically used on `people_index`, `resident_index`, or a type-specific index like `staff_index`.

---

## Rules (3)

### Rule: `group_dominant_room_rule`

Top room by time — 80%+ of qualified minutes in a single space, <= 5 unique rooms total. Each room must have >= 10 minutes total.

| Field | Value |
|-------|-------|
| Name | `group_dominant_room_rule` |
| Index | *(select target index)* |
| SQL Query | (below) |

```sql
WITH user_room_sessions AS (
    SELECT
        u.mac_address, r.space_name,
        EXTRACT(EPOCH FROM (u.end_time - u.start_time)) / 60.0 AS session_minutes
    FROM user_ap_trajectory u
    JOIN region_to_coverage r ON u.sensor_id = r.sensor
    WHERE u.mac_address IN (:index_values)
        AND r.space_name NOT IN ('NorthClassroom', 'SouthClassroom', 'InnerClassroom')
),
room_totals AS (
    SELECT mac_address, space_name,
           SUM(session_minutes) AS total_minutes
    FROM user_room_sessions
    GROUP BY mac_address, space_name
    HAVING SUM(session_minutes) >= 10
),
user_totals AS (
    SELECT mac_address, SUM(total_minutes) AS total_qualified_minutes
    FROM room_totals
    GROUP BY mac_address
),
top_room AS (
    SELECT mac_address, space_name, total_minutes,
           ROW_NUMBER() OVER (PARTITION BY mac_address ORDER BY total_minutes DESC) AS rn
    FROM room_totals
),
unique_room_count AS (
    SELECT mac_address, COUNT(DISTINCT space_name) AS room_count
    FROM user_room_sessions
    GROUP BY mac_address
)
SELECT tr.mac_address, tr.space_name AS top_space_name
FROM top_room tr
JOIN user_totals ut ON tr.mac_address = ut.mac_address
JOIN unique_room_count urc ON tr.mac_address = urc.mac_address
WHERE tr.rn = 1
    AND (tr.total_minutes * 1.0 / ut.total_qualified_minutes) >= 0.8
    AND urc.room_count <= 5
```

---

### Rule: `group_weekday_dominant_rule`

80%+ of weekday 10AM-5PM connections in one space_name.

| Field | Value |
|-------|-------|
| Name | `group_weekday_dominant_rule` |
| Index | *(select target index)* |
| SQL Query | (below) |

```sql
WITH weekday_sessions AS (
    SELECT u.mac_address, r.space_name
    FROM user_ap_trajectory u
    JOIN region_to_coverage r ON u.sensor_id = r.sensor
    WHERE u.mac_address IN (:index_values)
        AND EXTRACT(DOW FROM u.start_time) BETWEEN 1 AND 5
        AND u.start_time::time BETWEEN '10:00:00' AND '17:00:00'
        AND r.space_name NOT IN ('NorthClassroom', 'SouthClassroom', 'InnerClassroom')
),
session_counts AS (
    SELECT mac_address, COUNT(*) AS total_count
    FROM weekday_sessions
    GROUP BY mac_address
),
space_counts AS (
    SELECT mac_address, space_name, COUNT(*) AS space_count
    FROM weekday_sessions
    GROUP BY mac_address, space_name
),
space_pct AS (
    SELECT sc.mac_address, sc.space_name,
           sc.space_count * 1.0 / tc.total_count AS percentage,
           ROW_NUMBER() OVER (PARTITION BY sc.mac_address ORDER BY sc.space_count DESC) AS rn
    FROM space_counts sc
    JOIN session_counts tc ON sc.mac_address = tc.mac_address
)
SELECT mac_address, space_name
FROM space_pct
WHERE rn = 1 AND percentage >= 0.8
```

---

### Rule: `group_copair_rule`

Majority group from co-located devices — finds devices with 80%+ time overlap and returns the dominant space_name from those co-located connections.

| Field | Value |
|-------|-------|
| Name | `group_copair_rule` |
| Index | *(select target index)* |
| SQL Query | (below) |

```sql
WITH sessions AS (
    SELECT
        mac_address, sensor_id, start_time::date AS day,
        start_time, end_time,
        EXTRACT(EPOCH FROM (end_time - start_time)) AS duration
    FROM user_ap_trajectory
    WHERE mac_address IN (:index_values)
),
total_time AS (
    SELECT mac_address, SUM(duration) AS connection_time
    FROM sessions
    GROUP BY mac_address
),
raw_overlaps AS (
    SELECT
        t.mac_address AS target_user,
        u.mac_address AS other_user,
        EXTRACT(EPOCH FROM (LEAST(u.end_time, t.end_time) - GREATEST(u.start_time, t.start_time))) AS overlap_seconds,
        r.space_name
    FROM sessions t
    JOIN user_ap_trajectory u
        ON u.sensor_id = t.sensor_id
        AND u.start_time < t.end_time
        AND t.start_time < u.end_time
        AND u.mac_address != t.mac_address
    JOIN region_to_coverage r ON u.sensor_id = r.sensor
    WHERE r.space_name NOT IN ('NorthClassroom', 'SouthClassroom', 'InnerClassroom')
),
total_overlaps AS (
    SELECT r.target_user, r.other_user,
           SUM(r.overlap_seconds) AS total_overlap,
           tc.connection_time
    FROM raw_overlaps r
    JOIN total_time tc ON r.target_user = tc.mac_address
    GROUP BY r.target_user, r.other_user, tc.connection_time
),
strong_pairs AS (
    SELECT * FROM total_overlaps
    WHERE total_overlap * 1.0 / connection_time >= 0.8
),
matched_connections AS (
    SELECT r.target_user, r.space_name
    FROM raw_overlaps r
    JOIN strong_pairs sp ON r.target_user = sp.target_user AND r.other_user = sp.other_user
),
ranked_groups AS (
    SELECT target_user, space_name, COUNT(*) AS freq,
           ROW_NUMBER() OVER (PARTITION BY target_user ORDER BY COUNT(*) DESC, space_name ASC) AS rn
    FROM matched_connections
    GROUP BY target_user, space_name
)
SELECT target_user AS mac_address, space_name
FROM ranked_groups
WHERE rn = 1
```
