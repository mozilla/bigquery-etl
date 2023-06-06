-- Query for telemetry_derived.fog_decision_support_percentiles_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- See https://docs.google.com/document/d/1rznmBOZDiKB0U6485Gp7_TBgj0a2x5crmYSaaOSw7Ec/edit#
WITH metrics_base AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    client_info.client_id,
    ping_info.seq AS seq,
    metrics.counter.browser_engagement_active_ticks AS active_ticks,
    metrics.counter.browser_engagement_uri_count AS uri_count,
    TIMESTAMP_DIFF(
      TIMESTAMP_TRUNC(submission_timestamp, SECOND),
      ping_info.parsed_start_time,
      SECOND
    ) AS client_submission_latency,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    normalized_channel IN ('nightly', 'beta', 'release')
    AND DATE(submission_timestamp) = @submission_date
),
metrics_per_ping AS (
  SELECT
    channel,
    [
      STRUCT('client_submission_latency' AS metric_name, client_submission_latency AS value)
    ] AS metrics,
  FROM
    metrics_base
),
metrics_per_client AS (
  SELECT
    channel,
    [
      STRUCT('active_ticks' AS metric_name, SUM(active_ticks) AS value),
      STRUCT('uri_count' AS metric_name, SUM(uri_count) AS value)
    ] AS metrics,
  FROM
    metrics_base
  GROUP BY
    channel,
    client_id
),
metrics_per_seq AS (
  SELECT
    channel,
    [STRUCT('count_per_seq' AS metric_name, COUNT(*) AS value)] AS metrics,
  FROM
    metrics_base
  GROUP BY
    channel,
    client_id,
    seq
),
metrics_unioned AS (
  SELECT
    *
  FROM
    metrics_per_ping
  UNION ALL
  SELECT
    *
  FROM
    metrics_per_client
  UNION ALL
  SELECT
    *
  FROM
    metrics_per_seq
),
metrics_grouped AS (
  SELECT
    channel,
    metric_name,
    APPROX_QUANTILES(value, 20) AS percentiles,
    AVG(value) AS mean,
    STDDEV(value) AS std_dev,
  FROM
    metrics_unioned,
    UNNEST(metrics)
  GROUP BY
    channel,
    metric_name
),
metrics_windowed AS (
  SELECT
    channel,
    metric_name,
    PERCENTILE_DISC(value, 0.5) OVER (PARTITION BY channel, metric_name) AS median,
  FROM
    metrics_unioned,
    UNNEST(metrics)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY channel, metric_name) = 1
),
baseline_base AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    client_info.client_id,
    ping_info.seq AS seq,
    metrics.counter.browser_engagement_active_ticks AS active_ticks,
    metrics.counter.browser_engagement_uri_count AS uri_count,
    TIMESTAMP_DIFF(
      TIMESTAMP_TRUNC(submission_timestamp, SECOND),
      ping_info.parsed_start_time,
      SECOND
    ) AS client_submission_latency,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline`
  WHERE
    normalized_channel IN ('nightly', 'beta', 'release')
    AND DATE(submission_timestamp) = @submission_date
),
baseline_per_ping AS (
  SELECT
    channel,
    [
      STRUCT('client_submission_latency' AS metric_name, client_submission_latency AS value)
    ] AS metrics,
  FROM
    baseline_base
),
baseline_per_client AS (
  SELECT
    channel,
    [
      STRUCT('active_ticks' AS metric_name, SUM(active_ticks) AS value),
      STRUCT('uri_count' AS metric_name, SUM(uri_count) AS value)
    ] AS metrics,
  FROM
    baseline_base
  GROUP BY
    channel,
    client_id
),
baseline_per_seq AS (
  SELECT
    channel,
    [STRUCT('count_per_seq' AS metric_name, COUNT(*) AS value)] AS metrics,
  FROM
    baseline_base
  GROUP BY
    channel,
    client_id,
    seq
),
baseline_unioned AS (
  SELECT
    *
  FROM
    baseline_per_ping
  UNION ALL
  SELECT
    *
  FROM
    baseline_per_client
  UNION ALL
  SELECT
    *
  FROM
    baseline_per_seq
),
baseline_grouped AS (
  SELECT
    channel,
    metric_name,
    APPROX_QUANTILES(value, 20) AS percentiles,
    AVG(value) AS mean,
    STDDEV(value) AS std_dev,
  FROM
    baseline_unioned,
    UNNEST(metrics)
  GROUP BY
    channel,
    metric_name
),
baseline_windowed AS (
  SELECT
    channel,
    metric_name,
    PERCENTILE_DISC(value, 0.5) OVER (PARTITION BY channel, metric_name) AS median,
  FROM
    baseline_unioned,
    UNNEST(metrics)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY channel, metric_name) = 1
),
base AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    client_id,
    payload.info.subsession_counter AS seq,
    COALESCE(
      payload.processes.parent.scalars.browser_engagement_active_ticks,
      payload.simple_measurements.active_ticks
    ) AS active_ticks,
    payload.processes.parent.scalars.browser_engagement_total_uri_count_normal_and_private_mode AS uri_count,
    TIMESTAMP_DIFF(
      TIMESTAMP_TRUNC(submission_timestamp, SECOND),
      SAFE.PARSE_TIMESTAMP('%FT%R:%E*SZ', creation_date),
      SECOND
    ) AS client_submission_latency,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`
  WHERE
    normalized_channel IN ('nightly', 'beta', 'release')
    AND DATE(submission_timestamp) = @submission_date
),
per_ping AS (
  SELECT
    channel,
    [
      STRUCT('client_submission_latency' AS metric_name, client_submission_latency AS value)
    ] AS metrics,
  FROM
    base
),
per_client AS (
  SELECT
    channel,
    [
      STRUCT('active_ticks' AS metric_name, SUM(active_ticks) AS value),
      STRUCT('uri_count' AS metric_name, SUM(uri_count) AS value)
    ] AS metrics,
  FROM
    base
  GROUP BY
    channel,
    client_id
),
per_seq AS (
  SELECT
    channel,
    [STRUCT('count_per_seq' AS metric_name, COUNT(*) AS value)] AS metrics,
  FROM
    base
  GROUP BY
    channel,
    client_id,
    seq
),
unioned AS (
  SELECT
    *
  FROM
    per_ping
  UNION ALL
  SELECT
    *
  FROM
    per_client
  UNION ALL
  SELECT
    *
  FROM
    per_seq
),
grouped AS (
  SELECT
    channel,
    metric_name,
    APPROX_QUANTILES(value, 20) AS percentiles,
    AVG(value) AS mean,
    STDDEV(value) AS std_dev,
  FROM
    unioned,
    UNNEST(metrics)
  GROUP BY
    channel,
    metric_name
),
windowed AS (
  SELECT
    channel,
    metric_name,
    PERCENTILE_DISC(value, 0.5) OVER (PARTITION BY channel, metric_name) AS median,
  FROM
    unioned,
    UNNEST(metrics)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY channel, metric_name) = 1
)
SELECT
  @submission_date AS submission_date,
  "metrics" AS ping,
  *
FROM
  metrics_grouped
FULL JOIN
  metrics_windowed
USING
  (channel, metric_name)
UNION ALL
SELECT
  @submission_date AS submission_date,
  "baseline" AS ping,
  *
FROM
  baseline_grouped
FULL JOIN
  baseline_windowed
USING
  (channel, metric_name)
UNION ALL
SELECT
  @submission_date AS submission_date,
  "main" AS ping,
  *
FROM
  grouped
FULL JOIN
  windowed
USING
  (channel, metric_name)
ORDER BY
  metric_name,
  channel
