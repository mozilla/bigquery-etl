WITH filtered_data AS (
  SELECT
    sample_id,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    aggregates,
    os = 'Windows'
    AND channel = 'release' AS sampled
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_aggregates_v2`
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE
    first_bucket IS NOT NULL
    AND sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
),
static_combos AS (
  SELECT
    NULL AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    NULL AS os,
    '*' AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    '*' AS app_build_id
),
build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    filtered_data
  GROUP BY
    1,
    2
  HAVING
    -- Filter out builds having less than 0.5% of WAU
    -- for context see https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
    CASE
    WHEN
      channel = 'release'
    THEN
      COUNT(DISTINCT client_id) > 625000
    WHEN
      channel = 'beta'
    THEN
      COUNT(DISTINCT client_id) > 9000
    WHEN
      channel = 'nightly'
    THEN
      COUNT(DISTINCT client_id) > 375
    ELSE
      COUNT(DISTINCT client_id) > 100
    END
)
SELECT
  * EXCEPT (os, app_build_id),
  COALESCE(combo.os, table.os) AS os,
  COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
FROM
  filtered_data table
INNER JOIN
  build_ids
USING
  (app_build_id, channel)
CROSS JOIN
  static_combos combo