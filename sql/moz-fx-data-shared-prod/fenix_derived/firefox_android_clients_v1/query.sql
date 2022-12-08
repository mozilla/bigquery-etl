-- Query first observations for Firefox Android Clients.
WITH first_seen AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    country AS first_reported_country,
    isp AS first_reported_isp,
    DATETIME(first_run_date) AS first_run_datetime,
    normalized_channel AS channel,
    device_manufacturer,
    device_model,
    normalized_os_version AS os_version
  FROM
    `mozdata.fenix.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
    AND normalized_channel = 'release'
),
-- Find earliest data per client from the first_session ping.
first_session_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(SAFE.PARSE_DATETIME('%F', SUBSTR(client_info.first_run_date, 1, 10))) AS first_run_datetime,
    ARRAY_AGG(metrics.string.first_session_campaign ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_campaign,
    ARRAY_AGG(metrics.string.first_session_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.first_session_adgroup ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_ad_group,
    ARRAY_AGG(metrics.string.first_session_creative ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_creative
  FROM
    `mozdata.fenix.first_session`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) >= DATE(
      submission_timestamp
    ) -- first_session ping received after the client is first seen.
    AND ping_info.seq = 0 -- Pings are sent in sequence, this guarantees that the first one is returned.
  GROUP BY
    client_id
),
-- Find earliest data per client from the metrics ping.
metrics_ping AS (
  -- Fenix Release
  SELECT
    client_info.client_id AS client_id,
    DATETIME(MIN(submission_timestamp)) AS min_submission_datetime,
    ARRAY_AGG(metrics.string.metrics_adjust_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
),
_current AS (
  SELECT
    COALESCE(first_seen.client_id, first_session.client_id, metrics.client_id) AS client_id,
    first_seen.sample_id AS sample_id,
    first_seen.first_seen_date AS first_seen_date,
    DATE(first_seen.first_run_datetime) AS first_run_date,
    first_seen.first_reported_country AS first_reported_country,
    first_seen.first_reported_isp AS first_reported_isp,
    first_seen.channel AS channel,
    first_seen.device_manufacturer AS device_manufacturer,
    first_seen.device_model AS device_model,
    first_seen.os_version AS os_version,
    first_session.adjust_campaign AS adjust_campaign,
    first_session.adjust_ad_group AS adjust_ad_group,
    first_session.adjust_creative AS adjust_creative,
    COALESCE(first_session.adjust_network, metrics.adjust_network) AS adjust_network,
    metrics.install_source AS install_source,
    STRUCT(
      CASE
      WHEN
        first_session.client_id IS NULL
      THEN
        FALSE
      ELSE
        TRUE
      END
      AS reported_first_session_ping,
      CASE
      WHEN
        metrics.client_id IS NULL
      THEN
        FALSE
      ELSE
        TRUE
      END
      AS reported_metrics_ping,
      DATE(first_session.first_run_datetime) AS min_first_session_ping_run_date,
      CASE
        mozfun.norm.get_earliest_value(
          [
            (
              STRUCT(CAST(first_session.adjust_network AS STRING), first_session.first_run_datetime)
            ),
            (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
          ]
        )
      WHEN
        STRUCT(first_session.adjust_network, first_session.first_run_datetime)
      THEN
        'first_session'
      WHEN
        STRUCT(metrics.adjust_network, metrics.min_submission_datetime)
      THEN
        'metrics'
      ELSE
        NULL
      END
      AS adjust_network__source_ping,
      CASE
      WHEN
        metrics.install_source IS NOT NULL
      THEN
        'metrics'
      ELSE
        NULL
      END
      AS install_source__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (STRUCT(CAST(first_session.adjust_network AS STRING), first_session.first_run_datetime)),
          (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
        ]
      ).earliest_date AS adjust_network__source_ping_datetime,
      CASE
      WHEN
        metrics.install_source IS NOT NULL
      THEN
        metrics.min_submission_datetime
      ELSE
        NULL
      END
      AS install_source__source_ping_datetime
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
),
--existing clients in firefox_android_clients_v1
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
)
SELECT
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    COALESCE(_previous.device_manufacturer, _current.device_manufacturer) AS device_manufacturer,
    COALESCE(_previous.device_model, _current.device_model) AS device_model,
    COALESCE(_previous.os_version, _current.os_version) AS os_version,
    COALESCE(_previous.adjust_campaign, _current.adjust_campaign) AS adjust_campaign,
    COALESCE(_previous.adjust_ad_group, _current.adjust_ad_group) AS adjust_ad_group,
    COALESCE(_previous.adjust_creative, _current.adjust_creative) AS adjust_creative,
    COALESCE(_previous.adjust_network, _current.adjust_network) AS adjust_network,
    COALESCE(_previous.install_source, _current.install_source) AS install_source,
    STRUCT(
      COALESCE(
        _previous.metadata.reported_first_session_ping,
        _current.metadata.reported_first_session_ping
      ) AS reported_first_session_ping,
      COALESCE(
        _previous.metadata.reported_metrics_ping,
        _current.metadata.reported_metrics_ping
      ) AS reported_metrics_ping,
      COALESCE(
        _previous.metadata.min_first_session_ping_run_date,
        _current.metadata.min_first_session_ping_run_date
      ) AS min_first_session_ping_run_date,
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
  )
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  (client_id)
LEFT JOIN
  first_session_ping AS first_session
USING
  (client_id)
LEFT JOIN
  metrics_ping AS metrics
USING
  (client_id)
