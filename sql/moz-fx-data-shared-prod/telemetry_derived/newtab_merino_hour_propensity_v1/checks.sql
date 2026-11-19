-- macro checks

#fail
{{ not_null(["hour", "impressions", "adjusted_impressions", "clicks", "weight"], "snapshot_date = @snapshot_date") }}

#fail
{{ min_row_count(24, "snapshot_date = @snapshot_date") }}
-- Two one-sided bounds rather than one in_range(0, 23): the two-sided form of the
-- macro renders IS NOT BETWEEN, which the SQL parser used for DAG generation rejects.

#fail
{{ in_range(["hour"], 0, none, "snapshot_date = @snapshot_date") }}

#fail
{{ in_range(["hour"], none, 23, "snapshot_date = @snapshot_date") }}

#fail
{{ is_unique(["country", "hour"], "snapshot_date = @snapshot_date") }}
-- The global (country IS NULL) set must cover all 24 hours. Consumers have no
-- cross-country fallback for this weight -- a UTC-hour curve does not transfer between
-- regions -- so the global set is the only one that is unrecoverable if incomplete.

#fail
WITH global_hours AS (
  SELECT
    COUNT(DISTINCT `hour`) AS hours
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    snapshot_date = @snapshot_date
    AND country IS NULL
)
SELECT
  IF(
    (SELECT hours FROM global_hours) <> 24,
    ERROR(
      CONCAT(
        "Expected 24 global (country IS NULL) hour rows for this snapshot_date, found ",
        CAST((SELECT hours FROM global_hours) AS STRING)
      )
    ),
    NULL
  );

-- A per-country set may legitimately be short: query.py drops any hour cell below
-- MIN_CELL_IMPRESSIONS or with no clicks, and consumers leave those hours unadjusted.
-- So this warns rather than fails -- a partial country set is degraded, not broken.

#warn
WITH hours_per_country AS (
  SELECT
    country,
    COUNT(DISTINCT `hour`) AS hours
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    snapshot_date = @snapshot_date
    AND country IS NOT NULL
  GROUP BY
    country
),
offenders AS (
  SELECT
    CONCAT(country, '=', CAST(hours AS STRING)) AS detail
  FROM
    hours_per_country
  WHERE
    hours <> 24
)
SELECT
  IF(
    (SELECT COUNT(*) FROM offenders) > 0,
    ERROR(
      CONCAT(
        "Country sets with fewer than 24 hours: ",
        (SELECT ARRAY_TO_STRING(ARRAY_AGG(detail ORDER BY detail), ", ") FROM offenders)
      )
    ),
    NULL
  );

-- Normalization contract: dividing exposure by these weights must conserve total
-- adjusted exposure, so overall CTR is preserved rather than rescaled.

#fail
WITH per_country AS (
  SELECT
    country,
    SUM(adjusted_impressions) AS exposure,
    SUM(SAFE_DIVIDE(adjusted_impressions, weight)) AS reweighted_exposure
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    snapshot_date = @snapshot_date
  GROUP BY
    country
),
offenders AS (
  SELECT
    COALESCE(country, 'GLOBAL') AS country
  FROM
    per_country
  WHERE
    exposure > 0
    AND ABS(reweighted_exposure - exposure) / exposure > 0.005
)
SELECT
  IF(
    (SELECT COUNT(*) FROM offenders) > 0,
    ERROR(
      CONCAT(
        "Hour weights do not preserve adjusted exposure within 0.5% for: ",
        (SELECT ARRAY_TO_STRING(ARRAY_AGG(country), ", ") FROM offenders)
      )
    ),
    NULL
  );

-- Sanity band on the weights themselves. The measured hour swing is roughly 1.9x,
-- implying weights in the 0.75-1.55 range. Anything far outside that is more likely a
-- telemetry or upstream-weight problem than a behavioural one.

#warn
{{ in_range(["weight"], 0.4, none, "snapshot_date = @snapshot_date") }}

#warn
{{ in_range(["weight"], none, 2.5, "snapshot_date = @snapshot_date") }}
-- Snapshot stability: on a rolling multi-week window a single new day should move any
-- one hour's weight only slightly. A large day-over-day jump usually means a telemetry
-- or pipeline problem.

#warn
WITH moves AS (
  SELECT
    COALESCE(curr.country, 'GLOBAL') AS country,
    curr.hour,
    ABS(curr.weight - prev.weight) / prev.weight AS relative_move
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` curr
  JOIN
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` prev
    ON COALESCE(curr.country, '') = COALESCE(prev.country, '')
    AND curr.hour = prev.hour
    AND prev.snapshot_date = DATE_SUB(@snapshot_date, INTERVAL 1 DAY)
  WHERE
    curr.snapshot_date = @snapshot_date
    AND prev.weight > 0
),
offenders AS (
  SELECT
    CONCAT(country, ':', CAST(`hour` AS STRING)) AS cell
  FROM
    moves
  WHERE
    relative_move > 0.15
)
SELECT
  IF(
    (SELECT COUNT(*) FROM offenders) > 0,
    ERROR(
      CONCAT(
        "Hour weights moved more than 15% day-over-day for: ",
        (SELECT ARRAY_TO_STRING(ARRAY_AGG(cell ORDER BY cell LIMIT 20), ", ") FROM offenders)
      )
    ),
    NULL
  );
