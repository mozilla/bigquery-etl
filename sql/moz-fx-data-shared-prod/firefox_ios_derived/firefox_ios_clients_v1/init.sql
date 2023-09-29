------
-- PLEASE create the table using the ./bqetl deploy command
-- then run this query and append the results to it.
--
-- Running init.sql to generate the initial table vs schema + backfill approach
-- allows us to save a lot of time as we do not have to re-run the query for each partition date
-- instead all partitions to date could be used to generate the table and have the ETL take over.
------
-- Query first observations for Firefox iOS Clients.
WITH first_seen AS (
  SELECT
    client_id,
    submission_date,
    first_seen_date,
    sample_id,
    country AS first_reported_country,
    isp AS first_reported_isp,
    normalized_channel AS channel,
    device_manufacturer,
    device_model,
    normalized_os_version AS os_version,
    app_display_version AS app_version
  FROM
    firefox_ios.baseline_clients_first_seen
  WHERE
    submission_date < CURRENT_DATE
    AND client_id IS NOT NULL
),
-- Find the most recent activation record per client_id.
activations AS (
  SELECT
    client_id,
    is_activated,
  FROM
    firefox_ios_derived.new_profile_activation_v2
),
-- Find earliest data per client from the first_session ping.
first_session_ping_base AS (
  SELECT
    client_info.client_id,
    sample_id,
    submission_timestamp,
    NULLIF(metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(metrics.string.adjust_network, "") AS adjust_network,
  FROM
    firefox_ios.first_session
  WHERE
    DATE(submission_timestamp) < CURRENT_DATE
    AND client_info.client_id IS NOT NULL
),
first_session_ping AS (
  SELECT
    client_id,
    sample_id,
    ARRAY_AGG(
      IF(
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          submission_timestamp,
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    first_session_ping_base
  GROUP BY
    client_id,
    sample_id
),
-- Find earliest data per client from the metrics ping.
metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    submission_timestamp,
    NULLIF(fxa_metrics.metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(fxa_metrics.metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(fxa_metrics.metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(fxa_metrics.metrics.string.adjust_network, "") AS adjust_network,
  FROM
    firefox_ios.metrics AS fxa_metrics
  WHERE
    DATE(submission_timestamp) < CURRENT_DATE
    AND client_info.client_id IS NOT NULL
),
metrics_ping AS (
  SELECT
    client_id,
    sample_id,
    ARRAY_AGG(
      IF(
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          submission_timestamp,
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id
),
_current AS (
  SELECT
    * EXCEPT (adjust_info, sample_id),
    COALESCE(first_seen.sample_id, first_session.sample_id, metrics.sample_id) AS sample_id,
    COALESCE(first_session.adjust_info, metrics.adjust_info) AS adjust_info,
    STRUCT(
      IF(first_session.client_id IS NULL, FALSE, TRUE) AS is_reported_first_session_ping,
      IF(metrics.client_id IS NULL, FALSE, TRUE) AS is_reported_metrics_ping,
      CASE
        WHEN first_session.adjust_info IS NOT NULL
          THEN "first_session"
        WHEN metrics.adjust_info IS NOT NULL
          THEN "metrics"
        ELSE NULL
      END AS adjust_info__source_ping
    ) AS metadata,
    -- field to help us identify suspicious devices on iOS, for more info see: bug-1846554
    (app_version = '107.2' AND submission_date >= '2023-02-01') AS is_suspicious_device_client,
  FROM
    first_seen
  FULL OUTER JOIN
    first_session_ping AS first_session
  USING
    (client_id, sample_id)
  FULL OUTER JOIN
    metrics_ping AS metrics
  USING
    (client_id, sample_id)
  WHERE
    client_id IS NOT NULL
)
SELECT
  client_id,
  sample_id,
  first_seen_date,
  first_reported_country,
  first_reported_isp,
  channel,
  device_manufacturer,
  device_model,
  os_version,
  app_version,
  adjust_info.*,
  metadata,
  activations.is_activated,
  is_suspicious_device_client,
FROM
  _current
LEFT JOIN
  activations
USING
  (client_id)
