WITH client_counts AS (
  SELECT
    channel,
    app_version,
    COALESCE(os, "*") AS os,
    COALESCE(app_build_id, "*") AS app_build_id,
    process,
    metric,
    -- BigQuery has some null unicode characters which Postgresql doesn't like, so we remove those here.
    -- Also limit string length to 200 to match column length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS key,
    client_agg_type,
    metric_type,
    total_users,
    agg_type,
    aggregates,
    CASE
    WHEN
      channel = 'release'
    THEN
      total_users > 625000
    WHEN
      channel = 'beta'
    THEN
      total_users > 9000
    WHEN
      channel = 'nightly'
    THEN
      total_users > 375
    ELSE
      total_users > 100
    END
    AS user_count_check
  FROM
    `moz-fx-data-shared-prod.telemetry.client_probe_counts`
  WHERE
    channel = @channel
    AND app_version IS NOT NULL
)
SELECT
  channel,
  app_version,
  os,
  app_build_id,
  process,
  metric,
  key,
  client_agg_type,
  metric_type,
  total_users,
  -- Using MAX instead of COALESCE since this is not in the GROUP BY.
  MAX(IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(aggregates), NULL)) AS histogram,
  MAX(
    IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(aggregates), NULL)
  ) AS percentiles
FROM
  client_counts
WHERE
  user_count_check
GROUP BY
  channel,
  app_version,
  app_build_id,
  os,
  metric,
  metric_type,
  key,
  process,
  client_agg_type,
  total_users
