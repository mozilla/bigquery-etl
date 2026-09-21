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
-- Every emitted set, global and per-country alike, must cover all 24 hours. There is no
-- cross-country fallback for this weight (a UTC-hour curve does not transfer between
-- regions), so a country with a partial set silently loses the adjustment for the missing
-- hours rather than borrowing one.

#fail
WITH hours_per_set AS (
  SELECT
    COALESCE(country, 'GLOBAL') AS country,
    COUNT(DISTINCT `hour`) AS hours
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    snapshot_date = @snapshot_date
  GROUP BY
    country
),
offenders AS (
  SELECT
    CONCAT(country, '=', CAST(hours AS STRING)) AS detail
  FROM
    hours_per_set
  WHERE
    hours <> 24
)
SELECT
  IF(
    (SELECT COUNTIF(country = 'GLOBAL') FROM hours_per_set) = 0,
    ERROR("No global (country IS NULL) hour rows for this snapshot_date"),
    IF(
      (SELECT COUNT(*) FROM offenders) > 0,
      ERROR(
        CONCAT(
          "Expected 24 hours per emitted set, got: ",
          (SELECT ARRAY_TO_STRING(ARRAY_AGG(detail ORDER BY detail), ", ") FROM offenders)
        )
      ),
      NULL
    )
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
