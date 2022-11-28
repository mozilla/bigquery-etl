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
          min_first_session_ping_date DATE
        OPTIONS
          (description = "Date of the earliest first_session ping reported."),
          adjust_network__source_ping STRING
        OPTIONS
          (description = "Name of the ping that reported the first adjust_network value."),
          install_source__source_ping STRING
        OPTIONS
          (description = "Name of the ping that reports the install_source value."),
          adjust_network__source_ping_date DATE
        OPTIONS
          (description = "Date of the ping that reported the first adjust_network value."),
          install_source__source_ping_date DATE
        OPTIONS
          (description = "Date of the ping that reported the first install_source value.")
      >
  )
PARTITION BY
  first_seen_date
CLUSTER BY
  channel,
  first_reported_country,
  adjust_network,
  install_source
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
    `mozdata.org_mozilla_firefox.baseline_clients_first_seen`
  WHERE
    submission_date >= '2021-01-01'
    AND first_seen_date = submission_date
),
-- Find earliest first_session ping per client.
first_session_ping AS (
  SELECT
    client_info.client_id AS client_id,
    MIN(SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10))) AS first_run_date,
    ANY_VALUE(normalized_country_code) AS first_reported_country,
    ANY_VALUE(metadata.isp.name) AS first_reported_isp,
    ANY_VALUE(normalized_channel) AS channel,
    ANY_VALUE(client_info.device_manufacturer) AS device_manufacturer,
    ANY_VALUE(client_info.device_model) AS device_model,
    ANY_VALUE(client_info.os_version) AS os_version,
    ANY_VALUE(metrics.string.first_session_campaign) AS adjust_campaign,
    ANY_VALUE(metrics.string.first_session_network) AS adjust_network,
    ANY_VALUE(metrics.string.first_session_adgroup) AS adjust_ad_group,
    ANY_VALUE(metrics.string.first_session_creative) AS adjust_creative
  FROM
    `mozdata.org_mozilla_firefox.first_session`
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND DATE(submission_timestamp) >= SAFE.PARSE_DATE(
      '%F',
      SUBSTR(client_info.first_run_date, 1, 10)
    ) --Guarantees that the query retrieves only pings received after the first_run_date. Any previous ping data would be invalid.
    AND ping_info.seq = 0  -- Pings are sent in sequence, this condition guarantees that the first one is retrieved
  GROUP BY
    client_id
),
-- Find earliest metrics ping per client.
metrics_ping AS (
  -- Firefox Preview beta
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_fenix.metrics AS m
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND m.metrics.string.metrics_install_source IS NOT NULL
    AND m.metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
  UNION ALL
  -- Firefox Preview nightly
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_fenix_nightly.metrics AS m
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND m.metrics.string.metrics_install_source IS NOT NULL
    AND m.metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
  UNION ALL
  -- Fenix nightly
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_fennec_aurora.metrics AS m
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND m.metrics.string.metrics_install_source IS NOT NULL
    AND m.metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
  UNION ALL
  -- Fenix beta
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_firefox_beta.metrics AS m
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
    AND m.metrics.string.metrics_install_source IS NOT NULL
    AND m.metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
  UNION ALL
  -- Fenix Release
  SELECT
    client_info.client_id AS client_id,
    MIN(submission_timestamp) AS min_submission_timestamp,
    ARRAY_AGG(metrics.string.metrics_adjust_network IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS adjust_network,
    ARRAY_AGG(metrics.string.metrics_install_source IGNORE NULLS ORDER BY submission_timestamp ASC)[
      SAFE_OFFSET(0)
    ] AS install_source
  FROM
    org_mozilla_firefox.metrics AS m
  WHERE
    DATE(submission_timestamp) >= '2021-01-01'
  GROUP BY
    client_id
),
-- Get first adjust data per client_id per date
metrics_unique_attribution AS (
  SELECT
    client_id,
    DATE(min_submission_timestamp) AS metrics_ping_submission_date,
    ANY_VALUE(adjust_network) AS adjust_network,
    ANY_VALUE(install_source) AS install_source
  FROM
    metrics_ping
  GROUP BY
    client_id,
    metrics_ping_submission_date
)
SELECT
  base.client_id,
  base.first_seen_date,
  first_session.first_run_date,
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
    first_session.first_run_date AS min_first_session_ping_date,
    CASE
    WHEN
      first_session.adjust_network IS NOT NULL
    THEN
      'first_session'
    WHEN
      first_session.adjust_network IS NULL
      AND metrics.adjust_network IS NOT NULL
    THEN
      'metrics'
    ELSE
      NULL
    END
    AS adjust_network__source_ping,
    'metrics' AS install_source__source_ping,
    CASE
    WHEN
      first_session.adjust_network IS NOT NULL
    THEN
      first_session.first_run_date
    WHEN
      first_session.adjust_network IS NULL
      AND metrics.adjust_network IS NOT NULL
    THEN
      metrics.metrics_ping_submission_date
    ELSE
      NULL
    END
    AS adjust_network__source_ping_date,
    CASE
    WHEN
      metrics.adjust_network IS NOT NULL
    THEN
      metrics.metrics_ping_submission_date
    ELSE
      NULL
    END
    AS install_source__source_ping_date
  ) AS metadata
FROM
  first_seen AS base
LEFT JOIN
  first_session_ping AS first_session
USING
  (client_id)
LEFT JOIN
  metrics_unique_attribution AS metrics
USING
  (client_id)
