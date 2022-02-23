WITH histogram_data AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    key,
    h1.aggregates
  FROM
    telemetry_derived.clients_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) h1
  WHERE
    submission_date = @submission_date
),
scalars_data AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    scalar_aggregates
  FROM
    telemetry_derived.clients_scalar_aggregates_v1
  WHERE
    submission_date = @submission_date
)
SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  histogram_data.key,
  SUM(v1.value) AS total_sample
FROM
  histogram_data,
  UNNEST(aggregates) v1
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  key
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  app_build_id,
  channel,
  metric,
  histogram_data.key,
  SUM(v1.value) AS total_sample
FROM
  histogram_data,
  UNNEST(aggregates) v1
GROUP BY
  app_version,
  app_build_id,
  channel,
  metric,
  key
UNION ALL
SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  histogram_data.key,
  SUM(v1.value) AS total_sample
FROM
  histogram_data,
  UNNEST(aggregates) v1
GROUP BY
  os,
  app_version,
  channel,
  metric,
  key
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  histogram_data.key,
  SUM(v1.value) AS total_sample
FROM
  histogram_data,
  UNNEST(aggregates) v1
GROUP BY
  app_version,
  channel,
  metric,
  key
UNION ALL
SELECT
  os,
  app_version,
  app_build_id,
  channel,
  s1.metric,
  s1.key,
  CASE
  WHEN
    s1.agg_type IN ('count', 'true', 'false')
  THEN
    SUM(value)
  ELSE
    NULL
  END
  AS total_sample
FROM
  scalars_data,
  UNNEST(scalar_aggregates) s1
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  s1.metric,
  s1.key,
  s1.agg_type
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  app_build_id,
  channel,
  s1.metric,
  s1.key,
  CASE
  WHEN
    s1.agg_type IN ('count', 'true', 'false')
  THEN
    SUM(value)
  ELSE
    NULL
  END
  AS total_sample
FROM
  scalars_data,
  UNNEST(scalar_aggregates) s1
GROUP BY
  app_version,
  app_build_id,
  channel,
  s1.metric,
  s1.key,
  s1.agg_type
UNION ALL
SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  s1.metric,
  s1.key,
  CASE
  WHEN
    s1.agg_type IN ('count', 'true', 'false')
  THEN
    SUM(value)
  ELSE
    NULL
  END
  AS total_sample
FROM
  scalars_data,
  UNNEST(scalar_aggregates) s1
GROUP BY
  os,
  app_version,
  channel,
  s1.metric,
  s1.key,
  s1.agg_type
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  s1.metric,
  s1.key,
  CASE
  WHEN
    s1.agg_type IN ('count', 'true', 'false')
  THEN
    SUM(value)
  ELSE
    NULL
  END
  AS total_sample
FROM
  scalars_data,
  UNNEST(scalar_aggregates) s1
GROUP BY
  app_version,
  channel,
  s1.metric,
  s1.key,
  s1.agg_type
