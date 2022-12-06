-- Query first observations for Firefox Android Clients.
WITH first_seen AS (
  SELECT
    client_id,
    first_seen_date
  FROM
    `mozdata.fenix.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
),
-- Find earliest first_session ping per client.
first_session_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(SAFE.PARSE_DATETIME('%F', SUBSTR(client_info.first_run_date, 1, 10))) AS first_run_datetime,
    ARRAY_AGG(normalized_country_code ORDER BY metrics.datetime.first_session_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS first_reported_country,
    ARRAY_AGG(metadata.isp.name ORDER BY metrics.datetime.first_session_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS first_reported_isp,
    ARRAY_AGG(normalized_channel ORDER BY metrics.datetime.first_session_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(
      client_info.device_manufacturer
      ORDER BY
        metrics.datetime.first_session_timestamp ASC
    )[SAFE_OFFSET(0)] AS device_manufacturer,
    ARRAY_AGG(client_info.device_model ORDER BY metrics.datetime.first_session_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS device_model,
    ARRAY_AGG(client_info.os_version ORDER BY metrics.datetime.first_session_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS os_version,
    ARRAY_AGG(
      metrics.string.first_session_campaign
      ORDER BY
        metrics.datetime.first_session_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_campaign,
    ARRAY_AGG(
      metrics.string.first_session_network
      ORDER BY
        metrics.datetime.first_session_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(
      metrics.string.first_session_adgroup
      ORDER BY
        metrics.datetime.first_session_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_ad_group,
    ARRAY_AGG(
      metrics.string.first_session_creative
      ORDER BY
        metrics.datetime.first_session_timestamp ASC
    )[SAFE_OFFSET(0)] AS adjust_creative
  FROM
    `mozdata.fenix.first_session`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) >= DATE(
      submission_timestamp
    ) -- first_session ping received after the client is first seen.
    AND ping_info.seq = 0  -- Pings are sent in sequence, this guarantees that the first one is returned.
  GROUP BY
    client_id
),
-- Find earliest metrics ping per client.
metrics_ping AS (
  -- Firefox Preview beta
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
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
  -- Firefox Preview nightly
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
    org_mozilla_fenix_nightly.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
  -- Fenix nightly
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
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
  -- Fenix beta
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
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  UNION ALL
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
    first_seen.client_id,
    first_seen.first_seen_date,
    DATE(first_session.first_run_datetime) AS first_run_date,
    first_session.first_reported_country AS first_reported_country,
    first_session.first_reported_isp AS first_reported_isp,
    first_session.channel AS channel,
    first_session.device_manufacturer AS device_manufacturer,
    first_session.device_model AS device_model,
    first_session.os_version AS os_version,
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
      'metrics' AS install_source__source_ping,
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
  LEFT JOIN
    first_session_ping first_session
  USING
    (client_id)
  LEFT JOIN
    metrics_ping AS metrics
  USING
    (client_id)
),
--existing firefox_android_clients_v1
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
)
SELECT
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    COALESCE(
      _previous.device_manufacturer,
      first_session.device_manufacturer
    ) AS device_manufacturer,
    COALESCE(_previous.device_model, first_session.device_model) AS device_model,
    COALESCE(_previous.os_version, first_session.os_version) AS os_version,
    COALESCE(_previous.adjust_campaign, first_session.adjust_campaign) AS adjust_campaign,
    COALESCE(_previous.adjust_ad_group, first_session.adjust_ad_group) AS adjust_ad_group,
    COALESCE(_previous.adjust_creative, first_session.adjust_creative) AS adjust_creative,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(_previous.adjust_network AS STRING),
            _previous.metadata.adjust_network__source_ping_datetime
          )
        ),
        (STRUCT(CAST(first_session.adjust_network AS STRING), first_session.first_run_datetime)),
        (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
      ]
    ).earliest_value AS adjust_network,
    mozfun.norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST(_previous.install_source AS STRING),
            _previous.metadata.install_source__source_ping_datetime
          )
        ),
        (STRUCT(CAST(metrics.install_source AS STRING), metrics.min_submission_datetime))
      ]
    ).earliest_value AS install_source,
    STRUCT(
      CASE
      WHEN
        _previous.metadata.reported_first_session_ping
        OR first_session.client_id IS NOT NULL
      THEN
        TRUE
      ELSE
        FALSE
      END
      AS reported_first_session_ping,
      CASE
      WHEN
        _previous.metadata.reported_metrics_ping
        OR metrics.client_id IS NOT NULL
      THEN
        TRUE
      ELSE
        FALSE
      END
      AS reported_metrics_ping,
      LEAST(
        _previous.first_run_date,
        DATE(first_session.first_run_datetime)
      ) AS min_first_session_ping_run_date,
      CASE
        mozfun.norm.get_earliest_value(
          [
            (
              STRUCT(
                CAST(_previous.adjust_network AS STRING),
                _previous.metadata.adjust_network__source_ping_datetime
              )
            ),
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
        _previous.metadata.adjust_network__source_ping
      END
      AS adjust_network__source_ping,
      'metrics' AS install_source__source_ping,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              CAST(_previous.adjust_network AS STRING),
              _previous.metadata.adjust_network__source_ping_datetime
            )
          ),
          (STRUCT(CAST(first_session.adjust_network AS STRING), first_session.first_run_datetime)),
          (STRUCT(CAST(metrics.adjust_network AS STRING), metrics.min_submission_datetime))
        ]
      ).earliest_date AS adjust_network__source_ping_datetime,
      mozfun.norm.get_earliest_value(
        [
          (
            STRUCT(
              CAST(_previous.install_source AS STRING),
              _previous.metadata.install_source__source_ping_datetime
            )
          ),
          (STRUCT(CAST(metrics.install_source AS STRING), metrics.min_submission_datetime))
        ]
      ).earliest_date AS install_source__source_ping_datetime
    ) AS metadata
  )
FROM
  _current
JOIN
  _previous
USING
  (client_id)
LEFT JOIN
  first_session_ping first_session
USING
  (client_id)
LEFT JOIN
  metrics_ping AS metrics
USING
  (client_id)
