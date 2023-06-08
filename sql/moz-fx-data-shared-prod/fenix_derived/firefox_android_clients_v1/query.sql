-- Query first observations for Firefox Android Clients.
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
    submission_date = @submission_date
    AND normalized_channel = 'release'
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
-- Find the most recent activation record per client_id.
activations AS (
  SELECT
    client_id,
    ARRAY_AGG(activated ORDER BY submission_date DESC)[SAFE_OFFSET(0)] > 0 AS activated
  FROM
    fenix.new_profile_activation
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
-- Find earliest data per client from the first_session ping.
first_session_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(sample_id) AS sample_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
    MIN(SAFE.PARSE_DATETIME('%F', SUBSTR(client_info.first_run_date, 1, 10))) AS first_run_datetime,
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
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND ping_info.seq = 0 -- Pings are sent in sequence, this guarantees that the first one is returned.
  GROUP BY
    client_id
),
-- Find earliest data per client from the metrics ping.
metrics_ping AS (
  -- Fenix Release
  SELECT
    client_info.client_id AS client_id,
    MIN(sample_id) AS sample_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
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
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
),
-- Find most recent client details from the baseline ping.
baseline_ping AS (
  SELECT
    client_id,
    MAX(submission_date) AS last_reported_date,
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
),
_current AS (
  SELECT
    client_id,
    COALESCE(first_seen.sample_id, first_session.sample_id, metrics.sample_id) AS sample_id,
    first_seen.first_seen_date AS first_seen_date,
    first_seen.submission_date AS submission_date,
    DATE(first_seen.first_run_datetime) AS first_run_date,
    first_seen.first_reported_country AS first_reported_country,
    first_seen.first_reported_isp AS first_reported_isp,
    first_seen.channel AS channel,
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
    metrics.last_reported_adjust_campaign AS last_reported_adjust_campaign,
    metrics.last_reported_adjust_ad_group AS last_reported_adjust_ad_group,
    metrics.last_reported_adjust_creative AS last_reported_adjust_creative,
    metrics.last_reported_adjust_network AS last_reported_adjust_network,
    baseline.last_reported_date AS last_reported_date,
    baseline.last_reported_country AS last_reported_country,
    baseline.last_reported_device_model AS last_reported_device_model,
    baseline.last_reported_device_manufacturer AS last_reported_device_manufacturer,
    baseline.last_reported_locale AS last_reported_locale,
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
      DATE(first_session.min_submission_datetime) AS min_first_session_ping_submission_date,
      DATE(first_session.first_run_datetime) AS min_first_session_ping_run_date,
      DATE(metrics.min_submission_datetime) AS min_metrics_ping_submission_date,
      CASE
        mozfun.norm.get_earliest_value(
          [
            (
              STRUCT(
                CAST(first_session.adjust_network AS STRING),
                first_session.min_submission_datetime
              )
            ),
            (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
          ]
        )
        WHEN STRUCT(first_session.adjust_network, first_session.min_submission_datetime)
          THEN 'first_session'
        WHEN STRUCT(metrics.adjust_network, metrics.min_submission_datetime)
          THEN 'metrics'
        ELSE NULL
      END AS adjust_network__source_ping,
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
              first_session.min_submission_datetime
            )
          ),
          (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
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
  LEFT JOIN
    activations
  USING
    (client_id)
  WHERE
    client_id IS NOT NULL
),
--existing clients in firefox_android_clients_v1
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
)
SELECT
  client_id,
  COALESCE(_previous.sample_id, _current.sample_id) AS sample_id,
  COALESCE(_previous.first_seen_date, _current.first_seen_date) AS first_seen_date,
  COALESCE(_previous.submission_date, _current.submission_date) AS submission_date,
  COALESCE(_previous.first_run_date, _current.first_run_date) AS first_run_date,
  COALESCE(
    _previous.first_reported_country,
    _current.first_reported_country
  ) AS first_reported_country,
  COALESCE(_previous.first_reported_isp, _current.first_reported_isp) AS first_reported_isp,
  COALESCE(_previous.channel, _current.channel) AS channel,
  COALESCE(_previous.device_manufacturer, _current.device_manufacturer) AS device_manufacturer,
  COALESCE(_previous.device_model, _current.device_model) AS device_model,
  COALESCE(_previous.os_version, _current.os_version) AS os_version,
  COALESCE(_previous.app_version, _current.app_version) AS app_version,
  COALESCE(_previous.locale, _current.locale) AS locale,
  COALESCE(_previous.activated, _current.activated) AS activated,
  COALESCE(_previous.adjust_campaign, _current.adjust_campaign) AS adjust_campaign,
  COALESCE(_previous.adjust_ad_group, _current.adjust_ad_group) AS adjust_ad_group,
  COALESCE(_previous.adjust_creative, _current.adjust_creative) AS adjust_creative,
  COALESCE(_previous.adjust_network, _current.adjust_network) AS adjust_network,
  COALESCE(_previous.install_source, _current.install_source) AS install_source,
  COALESCE(
    _current.last_reported_adjust_campaign,
    _previous.last_reported_adjust_campaign
  ) AS last_reported_adjust_campaign,
  COALESCE(
    _current.last_reported_adjust_ad_group,
    _previous.last_reported_adjust_ad_group
  ) AS last_reported_adjust_ad_group,
  COALESCE(
    _current.last_reported_adjust_creative,
    _previous.last_reported_adjust_creative
  ) AS last_reported_adjust_creative,
  COALESCE(
    _current.last_reported_adjust_network,
    _previous.adjust_network
  ) AS last_reported_adjust_network,
  COALESCE(_current.last_reported_date, _previous.last_reported_date) AS last_reported_date,
  COALESCE(
    _current.last_reported_country,
    _previous.last_reported_country
  ) AS last_reported_country,
  COALESCE(
    _current.last_reported_device_model,
    _previous.last_reported_device_model
  ) AS last_reported_device_model,
  COALESCE(
    _current.last_reported_device_manufacturer,
    _previous.device_manufacturer
  ) AS last_reported_device_manufacturer,
  COALESCE(_current.last_reported_locale, _previous.locale) AS last_reported_locale,
  STRUCT(
    COALESCE(_previous.metadata.reported_first_session_ping, FALSE)
    OR COALESCE(
      _current.metadata.reported_first_session_ping,
      FALSE
    ) AS reported_first_session_ping,
    COALESCE(_previous.metadata.reported_metrics_ping, FALSE)
    OR COALESCE(_current.metadata.reported_metrics_ping, FALSE) AS reported_metrics_ping,
    CASE
      WHEN _previous.metadata.min_first_session_ping_submission_date IS NOT NULL
        AND _current.metadata.min_first_session_ping_submission_date IS NOT NULL
        THEN LEAST(
            _previous.metadata.min_first_session_ping_submission_date,
            _current.metadata.min_first_session_ping_submission_date
          )
      ELSE COALESCE(
          _previous.metadata.min_first_session_ping_submission_date,
          _current.metadata.min_first_session_ping_submission_date
        )
    END AS min_first_session_ping_submission_date,
    CASE
      WHEN _previous.metadata.min_first_session_ping_run_date IS NOT NULL
        AND _current.metadata.min_first_session_ping_run_date IS NOT NULL
        THEN LEAST(
            _previous.metadata.min_first_session_ping_run_date,
            _current.metadata.min_first_session_ping_run_date
          )
      ELSE COALESCE(
          _previous.metadata.min_first_session_ping_run_date,
          _current.metadata.min_first_session_ping_run_date
        )
    END AS min_first_session_ping_run_date,
    CASE
      WHEN _previous.metadata.min_metrics_ping_submission_date IS NOT NULL
        AND _current.metadata.min_metrics_ping_submission_date IS NOT NULL
        THEN LEAST(
            _previous.metadata.min_metrics_ping_submission_date,
            _current.metadata.min_metrics_ping_submission_date
          )
      ELSE COALESCE(
          _previous.metadata.min_metrics_ping_submission_date,
          _current.metadata.min_metrics_ping_submission_date
        )
    END AS min_metrics_ping_submission_date,
    COALESCE(
      _previous.metadata.adjust_network__source_ping,
      _current.metadata.adjust_network__source_ping
    ) AS adjust_network__source_ping,
    COALESCE(
      _previous.metadata.install_source__source_ping,
      _current.metadata.install_source__source_ping
    ) AS install_source__source_ping,
    COALESCE(
      _previous.metadata.adjust_network__source_ping_datetime,
      _current.metadata.adjust_network__source_ping_datetime
    ) AS adjust_network__source_ping_datetime,
    COALESCE(
      _previous.metadata.install_source__source_ping_datetime,
      _current.metadata.install_source__source_ping_datetime
    ) AS install_source__source_ping_datetime
  ) AS metadata
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (client_id)
