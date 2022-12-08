-- Initialization query first observations for Firefox Android Clients.
CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`(
    client_id STRING NOT NULL
    OPTIONS
      (description = "Unique ID for the client installation."),
      sample_id INTEGER NOT NULL
    OPTIONS
      (description = "Sample ID to limit query results during an analysis."),
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
  sample_id,
  first_reported_country,
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
    submission_date >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
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
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    client_id
)
SELECT
  first_seen.client_id AS client_id,
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
LEFT JOIN
  first_session_ping AS first_session
USING
  (client_id)
LEFT JOIN
  metrics_ping AS metrics
USING
  (client_id)
