-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    -- field to help us identify suspicious devices on iOS, for more info see: bug-1846554
    (
      app_display_version = '107.2'
      AND submission_date >= '2023-02-01'
    ) AS is_suspicious_device_client,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
),
first_session_ping_base AS (
  SELECT
    client_info.client_id,
    sample_id,
    normalized_channel,
    submission_timestamp,
    ping_info.seq AS ping_seq,
    NULLIF(metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(metrics.string.adjust_network, "") AS adjust_network,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.first_session`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
),
first_session_ping AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    ARRAY_AGG(
      IF(
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network,
          submission_timestamp AS adjust_attribution_timestamp
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    first_session_ping_base
  GROUP BY
    client_id,
    sample_id,
    normalized_channel
),
metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    normalized_channel,
    submission_timestamp,
    ping_info.seq AS ping_seq,
    NULLIF(metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(metrics.string.adjust_network, "") AS adjust_network,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.metrics` AS fxa_metrics
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
    AND client_info.client_id IS NOT NULL
),
metrics_ping AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    ARRAY_AGG(
      IF(
        adjust_ad_group IS NOT NULL
        OR adjust_campaign IS NOT NULL
        OR adjust_creative IS NOT NULL
        OR adjust_network IS NOT NULL,
        STRUCT(
          adjust_ad_group,
          adjust_campaign,
          adjust_creative,
          adjust_network,
          submission_timestamp AS adjust_attribution_timestamp
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS adjust_info,
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id,
    normalized_channel
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel,
  COALESCE(first_session_ping.adjust_info, metrics_ping.adjust_info) AS adjust_info,
  is_suspicious_device_client,
FROM
  new_profiles
LEFT JOIN
  first_session_ping
  USING (client_id, sample_id, normalized_channel)
LEFT JOIN
  metrics_ping
  USING (client_id, sample_id, normalized_channel)
