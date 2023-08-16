-- Initialization query first observations for Firefox Android Clients.
WITH baseline_clients AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    submission_date,
    country,
    isp AS first_reported_isp,
    DATETIME(first_run_date) AS first_run_datetime,
    normalized_channel AS channel,
    device_manufacturer,
    device_model,
    normalized_os_version AS os_version,
    app_display_version AS app_version,
    locale,
    is_new_profile,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
  WHERE
    submission_date >= '2020-01-21'
    AND client_id IS NOT NULL
),
first_seen AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    submission_date,
    country AS first_reported_country,
    first_reported_isp,
    first_run_datetime,
    channel,
    device_manufacturer,
    device_model,
    os_version,
    app_version,
    locale
  FROM
    baseline_clients
  WHERE
    is_new_profile
),
-- Find the most recent activation record per client_id. Data available since '2021-12-01'
activations AS (
  SELECT
    client_id,
    ARRAY_AGG(activated ORDER BY submission_date DESC)[SAFE_OFFSET(0)] > 0 AS activated,
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_activation`
  WHERE
    submission_date >= '2021-12-01'
  GROUP BY
    client_id
),
-- Find earliest data per client from the first_session ping.
first_session_ping_min_seq AS (
  SELECT
    client_id,
    sample_id,
    seq
  FROM
    (
      SELECT
        client_info.client_id AS client_id,
        sample_id,
        ping_info.seq AS seq,
        submission_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY
            client_info.client_id
          ORDER BY
            ping_info.seq,
            submission_timestamp
        ) AS RANK
      FROM
        fenix.first_session AS fenix_first_session
      WHERE
        ping_info.seq IS NOT NULL
        AND DATE(submission_timestamp) >= '2019-01-01'
    )
  WHERE
    RANK = 1 -- Pings are sent in sequence, this guarantees that the first one is returned.
  GROUP BY
    client_id,
    sample_id,
    seq
),
first_session_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(fenix_first_session.sample_id) AS sample_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
    MIN(SAFE.PARSE_DATETIME('%F', SUBSTR(client_info.first_run_date, 1, 10))) AS first_run_datetime,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(metrics.string.first_session_campaign IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_campaign,
    ARRAY_AGG(metrics.string.first_session_network IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.first_session_adgroup IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_ad_group,
    ARRAY_AGG(metrics.string.first_session_creative IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_creative
  FROM
    fenix.first_session AS fenix_first_session
  LEFT JOIN
    first_session_ping_min_seq
  ON
    (
      client_info.client_id = first_session_ping_min_seq.client_id
      AND ping_info.seq = first_session_ping_min_seq.seq
      AND fenix_first_session.sample_id = first_session_ping_min_seq.sample_id
    )
  WHERE
    DATE(submission_timestamp) >= '2019-01-01'
    AND (first_session_ping_min_seq.client_id IS NOT NULL OR ping_info.seq IS NULL)
  GROUP BY
    client_id
),
-- Find earliest data per client from the metrics ping.
metrics_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(sample_id) AS sample_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(
      metrics.string.metrics_adjust_campaign IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_campaign,
    ARRAY_AGG(metrics.string.metrics_adjust_network IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(
      metrics.string.metrics_adjust_ad_group IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_ad_group,
    ARRAY_AGG(
      metrics.string.metrics_adjust_creative IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_creative,
    ARRAY_AGG(metrics.string.metrics_install_source IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source,
    ARRAY_AGG(
      metrics.string.metrics_adjust_ad_group IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS last_reported_adjust_ad_group,
    ARRAY_AGG(
      metrics.string.metrics_adjust_creative IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS last_reported_adjust_creative,
    ARRAY_AGG(
      metrics.string.metrics_adjust_network IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS last_reported_adjust_network,
    ARRAY_AGG(
      metrics.string.metrics_adjust_campaign IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS last_reported_adjust_campaign,
  FROM
    fenix.metrics AS fenix_metrics
  WHERE
    DATE(submission_timestamp) >= '2019-06-21'
  GROUP BY
    client_id
),
-- Find most recent client details from the baseline ping.
baseline_ping AS (
  SELECT
    client_id,
    MAX(submission_date) AS last_reported_date,
    ARRAY_AGG(channel IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS last_reported_channel,
    ARRAY_AGG(country IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS last_reported_country,
    ARRAY_AGG(device_model IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS last_reported_device_model,
    ARRAY_AGG(device_manufacturer IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS last_reported_device_manufacturer,
    ARRAY_AGG(locale IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS last_reported_locale,
  FROM
    baseline_clients
  GROUP BY
    client_id
)
SELECT
  client_id,
  COALESCE(first_seen.sample_id, first_session.sample_id, metrics.sample_id) AS sample_id,
  first_seen.first_seen_date AS first_seen_date,
  first_seen.submission_date AS submission_date,
  DATE(first_seen.first_run_datetime) AS first_run_date,
  first_seen.first_reported_country AS first_reported_country,
  first_seen.first_reported_isp AS first_reported_isp,
  COALESCE(first_seen.channel, first_session.channel, metrics.channel) AS channel,
  first_seen.device_manufacturer AS device_manufacturer,
  first_seen.device_model AS device_model,
  first_seen.os_version AS os_version,
  first_seen.app_version AS app_version,
  first_seen.locale AS locale,
  activated AS activated,
  COALESCE(first_session.adjust_campaign, metrics.adjust_campaign) AS adjust_campaign,
  COALESCE(first_session.adjust_ad_group, metrics.adjust_ad_group) AS adjust_ad_group,
  COALESCE(first_session.adjust_creative, metrics.adjust_creative) AS adjust_creative,
  COALESCE(first_session.adjust_network, metrics.adjust_network) AS adjust_network,
  metrics.install_source AS install_source,
  COALESCE(
    metrics.last_reported_adjust_campaign,
    first_session.adjust_campaign
  ) AS last_reported_adjust_campaign,
  COALESCE(
    metrics.last_reported_adjust_ad_group,
    first_session.adjust_ad_group
  ) AS last_reported_adjust_ad_group,
  COALESCE(
    metrics.last_reported_adjust_creative,
    first_session.adjust_creative
  ) AS last_reported_adjust_creative,
  COALESCE(
    metrics.last_reported_adjust_network,
    first_session.adjust_network
  ) AS last_reported_adjust_network,
  COALESCE(baseline.last_reported_date, first_seen.first_seen_date) AS last_reported_date,
  COALESCE(baseline.last_reported_channel, first_seen.channel) AS last_reported_channel,
  COALESCE(
    baseline.last_reported_country,
    first_seen.first_reported_country
  ) AS last_reported_country,
  COALESCE(
    baseline.last_reported_device_model,
    first_seen.device_model
  ) AS last_reported_device_model,
  COALESCE(
    baseline.last_reported_device_manufacturer,
    first_seen.device_manufacturer
  ) AS last_reported_device_manufacturer,
  COALESCE(baseline.last_reported_locale, first_seen.locale) AS last_reported_locale,
  STRUCT(
    CASE
      WHEN first_session.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_first_session_ping,
    CASE
      WHEN metrics.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_metrics_ping,
    CASE
      WHEN first_seen.client_id IS NULL
        THEN FALSE
      ELSE TRUE
    END AS reported_baseline_ping,
    DATE(first_session.min_submission_datetime) AS min_first_session_ping_submission_date,
    DATE(first_session.first_run_datetime) AS min_first_session_ping_run_date,
    DATE(metrics.min_submission_datetime) AS min_metrics_ping_submission_date,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(first_session.adjust_network AS STRING),
            'first_session_ping',
            DATETIME(first_session.min_submission_datetime)
          )
        ),
        (
          STRUCT(
            CAST(metrics.adjust_network AS STRING),
            'metrics_ping',
            DATETIME(metrics.min_submission_datetime)
          )
        )
      ]
    ).earliest_value_source AS adjust_network__source_ping,
    CASE
      WHEN metrics.install_source IS NOT NULL
        THEN 'metrics'
      ELSE NULL
    END AS install_source__source_ping,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(first_session.adjust_network AS STRING),
            'first_session_ping',
            DATETIME(first_session.min_submission_datetime)
          )
        ),
        (
          STRUCT(
            CAST(metrics.adjust_network AS STRING),
            'metrics_ping',
            DATETIME(metrics.min_submission_datetime)
          )
        )
      ]
    ).earliest_date AS adjust_network__source_ping_datetime,
    CASE
      WHEN metrics.install_source IS NOT NULL
        THEN metrics.min_submission_datetime
      ELSE NULL
    END AS install_source__source_ping_datetime
  ) AS metadata
FROM
  first_seen
FULL OUTER JOIN
  first_session_ping first_session
USING
  (client_id)
FULL OUTER JOIN
  metrics_ping AS metrics
USING
  (client_id)
FULL OUTER JOIN
  baseline_ping AS baseline
USING
  (client_id)
FULL OUTER JOIN
  activations
USING
  (client_id)
WHERE
  client_id IS NOT NULL
