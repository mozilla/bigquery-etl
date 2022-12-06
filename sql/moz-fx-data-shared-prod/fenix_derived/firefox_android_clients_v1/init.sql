-- Initialization query first observations for Firefox Android Clients.
CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`(
    client_id STRING NOT NULL
    OPTIONS
      (description = "Unique ID for the client installation."),
      first_seen_date DATE
    OPTIONS
      (description = "Date when the browser first reported a baseline ping."),
      first_run_date DATE
    OPTIONS
      (description = "Date when the browser first ran."),
      first_reported_country STRING
    OPTIONS
      (description = "First reported country for the client installation"),
      first_reported_isp STRING
    OPTIONS
      (description = "Name of the first reported isp (Internet Service Provider)."),
      channel STRING
    OPTIONS
      (description = "Channel where the browser is released."),
      device_manufacturer STRING
    OPTIONS
      (description = "Manufacturer of the device where the client is installed."),
      device_model STRING
    OPTIONS
      (description = "Model of the device where the client is installed."),
      os_version STRING
    OPTIONS
      (description = "Version of the Operating System where the client is originally installed."),
      adjust_campaign STRING
    OPTIONS
      (description = "Structure parameter for the campaign name."),
      adjust_ad_group STRING
    OPTIONS
      (description = "Structure parameter for the the ad group of a campaign."),
      adjust_creative STRING
    OPTIONS
      (description = "Structure parameter for the creative content of a campaign."),
      adjust_network STRING
    OPTIONS
      (description = "The type of source of a client installation."),
      install_source STRING
    OPTIONS
      (description = "The source of a client installation."),
      metadata STRUCT<
        reported_first_session_ping BOOL
        OPTIONS
          (description = "True if the client ever reported a first_session ping."),
          reported_metrics_ping BOOL
        OPTIONS
          (description = "True if the client ever reported a metrics ping."),
          min_first_session_ping_run_date DATE
        OPTIONS
          (description = "Date of first run in the earliest first_session ping reported."),
          adjust_network__source_ping STRING
        OPTIONS
          (description = "Name of the ping that reported the first adjust_network value."),
          install_source__source_ping STRING
        OPTIONS
          (description = "Name of the ping that reports the install_source value."),
          adjust_network__source_ping_datetime DATETIME
        OPTIONS
          (description = "Datetime of the ping that reported the first adjust_network value."),
          install_source__source_ping_datetime DATETIME
        OPTIONS
          (description = "Datetime of the ping that reported the first install_source value.")
      >
  )
PARTITION BY
  first_seen_date
CLUSTER BY
  channel,
  first_reported_country,
  first_reported_isp,
  device_model
OPTIONS
  (
    description = "First observations for Firefox Android clients retrieved from the earliest pings: baseline, first_session and metrics. The attributes stored in this table include the first attribution, device, OS version and ISP. This table should be accessed through the user-facing view `fenix.firefox_android_clients`. Proposal: https://docs.google.com/document/d/12bj4DhCybelqHVgOVq8KJlzgtbbUw3f68palNrv-gaM/. For more details about attribution and campaign structure see https://help.adjust.com/en/article/tracker-urls#campaign-structure-parameters."
  );

INSERT
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
WITH first_seen AS (
  SELECT
    client_id,
    first_seen_date
  FROM
    `mozdata.fenix.baseline_clients_first_seen`
  WHERE
    submission_date >= '2021-01-01'
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
    DATE(submission_timestamp) = '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    client_id
)
SELECT
  first_seen.client_id,
  first_seen.first_seen_date,
  DATE(first_session.first_run_datetime) AS first_run_date,
  first_session.first_reported_country,
  first_session.first_reported_isp,
  first_session.channel,
  first_session.device_manufacturer,
  first_session.device_model,
  first_session.os_version,
  first_session.adjust_campaign,
  first_session.adjust_ad_group,
  first_session.adjust_creative,
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
          (STRUCT(CAST(first_session.adjust_network AS STRING), first_session.first_run_datetime)),
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
  first_session_ping AS first_session
USING
  (client_id)
LEFT JOIN
  metrics_ping AS metrics
USING
  (client_id)
