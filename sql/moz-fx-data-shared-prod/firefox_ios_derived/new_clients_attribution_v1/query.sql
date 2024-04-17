WITH first_seen AS (
  SELECT
    client_id,
    first_seen_date,
    sample_id,
    normalized_channel AS channel,
  FROM
    firefox_ios.baseline_clients_first_seen
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 3 DAY)
    AND is_new_profile
    AND client_id IS NOT NULL
),
first_session_ping_base AS (
  SELECT
    client_info.client_id,
    sample_id,
    normalized_channel AS channel,
    submission_timestamp,
    NULLIF(metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(metrics.string.adjust_network, "") AS adjust_network,
  FROM
    firefox_ios.first_session
  WHERE
    DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date
    AND client_info.client_id IS NOT NULL
),
first_session_ping AS (
  SELECT
    client_id,
    sample_id,
    channel,
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
    sample_id,
    channel
),
-- Find earliest data per client from the metrics ping.
metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    normalized_channel AS channel,
    submission_timestamp,
    NULLIF(fxa_metrics.metrics.string.adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(fxa_metrics.metrics.string.adjust_campaign, "") AS adjust_campaign,
    NULLIF(fxa_metrics.metrics.string.adjust_creative, "") AS adjust_creative,
    NULLIF(fxa_metrics.metrics.string.adjust_network, "") AS adjust_network,
  FROM
    firefox_ios.metrics AS fxa_metrics
  WHERE
    DATE(submission_timestamp) BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date
    AND client_info.client_id IS NOT NULL
),
metrics_ping AS (
  SELECT
    client_id,
    sample_id,
    channel,
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
    sample_id,
    channel
)
SELECT
  first_seen_date,
  client_id,
  sample_id,
  channel,
  COALESCE(first_session.adjust_info, metrics.adjust_info).* EXCEPT(submission_timestamp),
  STRUCT(
    CASE
      WHEN first_session.adjust_info IS NOT NULL
        THEN "first_session"
      WHEN metrics.adjust_info IS NOT NULL
        THEN "metrics"
      ELSE NULL
    END AS adjust_info__source_ping,
    CASE
      WHEN first_session.adjust_info IS NOT NULL
        THEN first_session.adjust_info.submission_timestamp
      WHEN metrics.adjust_info IS NOT NULL
        THEN metrics.adjust_info.submission_timestamp
      ELSE NULL
    END AS adjust_info__received_at
  ) AS metadata,
FROM
  first_seen
FULL OUTER JOIN
  first_session_ping AS first_session
  USING (client_id, sample_id, channel)
FULL OUTER JOIN
  metrics_ping AS metrics
  USING (client_id, sample_id, channel)
WHERE
  client_id IS NOT NULL
