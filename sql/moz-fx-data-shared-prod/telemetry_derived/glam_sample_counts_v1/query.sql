WITH all_samples AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    key,
    h1.aggregates
  FROM
    clients_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) h1
  WHERE
    submission_date = @submission_date
)
SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
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
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
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
  CAST(NULL AS INT64) AS app_version,
  app_build_id,
  channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  os,
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
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  os,
  app_version,
  channel,
  metric,
  key
UNION ALL
SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  os,
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
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  app_version,
  channel,
  metric,
  key
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  app_version,
  metric,
  key
UNION ALL
SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  os,
  metric,
  key
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  channel,
  metric,
  key
UNION ALL
SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  all_samples.key,
  SUM(v1.value) AS total_sample
FROM
  all_samples,
  UNNEST(aggregates) v1
GROUP BY
  metric,
  key
