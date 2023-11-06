-- Query first observations for Firefox Android Clients.
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
    app_display_version AS app_version,
    DATETIME(first_run_date) AS first_run_datetime,
    locale,
  FROM
    fenix.baseline_clients_first_seen
  WHERE
    submission_date = @submission_date
    AND client_id IS NOT NULL
),
activations AS (
  SELECT
    client_id,
    CAST(activated AS BOOLEAN) AS is_activated,
  FROM
    fenix.new_profile_activation
  WHERE
    submission_date = @submission_date
),
-- Find earliest data per client from the first_session ping.
first_session_ping_base AS (
  SELECT
    client_info.client_id,
    sample_id,
    submission_timestamp,
    NULLIF(metrics.string.first_session_adgroup, "") AS adjust_ad_group,
    NULLIF(metrics.string.first_session_campaign, "") AS adjust_campaign,
    NULLIF(metrics.string.first_session_creative, "") AS adjust_creative,
    NULLIF(metrics.string.first_session_network, "") AS adjust_network,
  FROM
    fenix.first_session
  WHERE
    DATE(submission_timestamp) = @submission_date
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
    NULLIF(fenix_metrics.metrics.string.metrics_adjust_ad_group, "") AS adjust_ad_group,
    NULLIF(fenix_metrics.metrics.string.metrics_adjust_campaign, "") AS adjust_campaign,
    NULLIF(fenix_metrics.metrics.string.metrics_adjust_creative, "") AS adjust_creative,
    NULLIF(fenix_metrics.metrics.string.metrics_adjust_network, "") AS adjust_network,
    NULLIF(fenix_metrics.metrics.string.metrics_install_source, "") AS install_source,
  FROM
    fenix.metrics AS fenix_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
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
    ARRAY_AGG(install_source IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source,
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id
),
_current AS (
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
    COALESCE(first_session.adjust_info, metrics.adjust_info) AS adjust_info,
    metrics.install_source,
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
),
_previous AS (
  SELECT
    *
  FROM
    fenix_derived.firefox_android_clients_v2
)
SELECT
  client_id,
  sample_id,
  COALESCE(_previous.first_seen_date, _current.first_seen_date) AS first_seen_date,
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
  activations.is_activated,
  -- below is to avoid mix and matching different adjust attributes
  -- from different records. This way we always treat them as a single "unit"
  IF(
    _previous.adjust_ad_group IS NULL
    AND _previous.adjust_campaign IS NULL
    AND _previous.adjust_creative IS NULL
    AND _previous.adjust_network IS NULL,
    _current.adjust_info,
    STRUCT(
      _previous.submission_timestamp,
      _previous.adjust_ad_group,
      _previous.adjust_campaign,
      _previous.adjust_creative,
      _previous.adjust_network
    )
  ).*,
  COALESCE(_previous.install_source, _current.install_source) AS install_source,
  STRUCT(
    COALESCE(
      _previous.metadata.reported_first_session_ping
      OR _current.metadata.is_reported_first_session_ping,
      FALSE
    ) AS reported_first_session_ping,
    COALESCE(
      _previous.metadata.reported_metrics_ping
      OR _current.metadata.is_reported_metrics_ping,
      FALSE
    ) AS reported_metrics_ping,
    COALESCE(
      _previous.metadata.adjust_info__source_ping,
      _current.metadata.adjust_info__source_ping
    ) AS adjust_info__source_ping
  ) AS metadata,
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (client_id, sample_id)
LEFT JOIN
  activations
USING
  (client_id)
