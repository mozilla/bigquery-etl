-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    distribution_id,
    install_source,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline_clients_first_seen`
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
    metrics.string.first_session_distribution_id AS distribution_id,
    NULLIF(metrics.string.first_session_adgroup, "") AS adjust_ad_group,
    NULLIF(metrics.string.first_session_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.first_session_creative, "") AS adjust_creative,
    NULLIF(metrics.string.first_session_network, "") AS adjust_network,
    NULLIF(metrics.string.play_store_attribution_campaign, "") AS play_store_attribution_campaign,
    NULLIF(metrics.string.play_store_attribution_medium, "") AS play_store_attribution_medium,
    NULLIF(metrics.string.play_store_attribution_source, "") AS play_store_attribution_source,
    NULLIF(metrics.string.play_store_attribution_content, "") AS play_store_attribution_content,
    NULLIF(metrics.string.play_store_attribution_term, "") AS play_store_attribution_term,
    NULLIF(
      metrics.text2.play_store_attribution_install_referrer_response,
      ""
    ) AS play_store_attribution_install_referrer_response,
    NULLIF(metrics.string.meta_attribution_app, "") AS meta_attribution_app,
  FROM
    `moz-fx-data-shared-prod.fenix.first_session`
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
    ARRAY_AGG(
      IF(
        play_store_attribution_campaign IS NOT NULL
        OR play_store_attribution_medium IS NOT NULL
        OR play_store_attribution_source IS NOT NULL
        OR play_store_attribution_content IS NOT NULL
        OR play_store_attribution_term IS NOT NULL
        OR play_store_attribution_install_referrer_response IS NOT NULL,
        STRUCT(
          play_store_attribution_campaign,
          play_store_attribution_medium,
          play_store_attribution_source,
          submission_timestamp AS play_store_attribution_timestamp,
          play_store_attribution_content,
          play_store_attribution_term,
          play_store_attribution_install_referrer_response
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS play_store_info,
    ARRAY_AGG(
      IF(
        meta_attribution_app IS NOT NULL,
        STRUCT(meta_attribution_app, submission_timestamp AS meta_attribution_timestamp),
        NULL
      ) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS meta_info,
    ARRAY_AGG(
      IF(distribution_id IS NOT NULL, distribution_id, NULL) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
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
    metrics.string.metrics_distribution_id AS distribution_id,
    NULLIF(metrics.string.metrics_install_source, "") AS install_source,
    NULLIF(metrics.string.metrics_adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(metrics.string.metrics_adjust_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.metrics_adjust_creative, "") AS adjust_creative,
    NULLIF(metrics.string.metrics_adjust_network, "") AS adjust_network,
  FROM
    `moz-fx-data-shared-prod.fenix.metrics` AS fxa_metrics
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
    ARRAY_AGG(
      IF(install_source IS NOT NULL, install_source, NULL) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS install_source,
    ARRAY_AGG(
      IF(distribution_id IS NOT NULL, distribution_id, NULL) IGNORE NULLS
      ORDER BY
        ping_seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
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
  COALESCE(new_profiles.install_source, metrics_ping.install_source) AS install_source,
  COALESCE(first_session_ping.adjust_info, metrics_ping.adjust_info) AS adjust_info,
  first_session_ping.play_store_info,
  first_session_ping.meta_info,
  COALESCE(
    first_session_ping.distribution_id,
    new_profiles.distribution_id,
    metrics_ping.distribution_id
  ) AS distribution_id,
FROM
  new_profiles
LEFT JOIN
  first_session_ping
  USING (client_id, sample_id, normalized_channel)
LEFT JOIN
  metrics_ping
  USING (client_id, sample_id, normalized_channel)
