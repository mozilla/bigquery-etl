-- Query first observations for Firefox Android Clients.
-- Proposal:
-- https://docs.google.com/document/d/12bj4DhCybelqHVgOVq8KJlzgtbbUw3f68palNrv-gaM/edit#
CREATE TEMP FUNCTION get_first_not_null_value(
  existing_value STRING,
  existing_date DATE,
  first_session_value STRING,
  first_session_date DATE,
  metrics_value STRING,
  metrics_date DATE
) AS (
  -- Return first not null value, or null.
  CASE
  WHEN
    existing_value IS NULL
  THEN
    COALESCE(first_session_value, metrics_value)
  WHEN
    existing_value IS NOT NULL
    AND first_session_value IS NOT NULL
    AND first_session_date < existing_date
  THEN
    first_session_value
  WHEN
    existing_value IS NOT NULL
    AND first_session_value IS NULL
    AND metrics_value IS NOT NULL
    AND metrics_date < existing_date
  THEN
    metrics_value
  ELSE
    existing_value
  END
);

CREATE TEMP FUNCTION get_first_not_null_date(
  existing_value STRING,
  existing_date DATE,
  first_session_value STRING,
  first_session_date DATE,
  metrics_value STRING,
  metrics_date DATE
) AS (
  -- Return the date of the first source ping where adjust_network is not null.
  CASE
  WHEN
    existing_value IS NULL
    AND first_session_value IS NOT NULL
  THEN
    first_session_date
  WHEN
    existing_value IS NULL
    AND first_session_value IS NULL
    AND metrics_value IS NOT NULL
  THEN
    metrics_date
  WHEN
    existing_value IS NOT NULL
    AND first_session_value IS NOT NULL
    AND first_session_date < existing_date
  THEN
    first_session_date
  WHEN
    existing_value IS NOT NULL
    AND first_session_value IS NULL
    AND metrics_value IS NOT NULL
    AND metrics_date < existing_date
  THEN
    metrics_date
  ELSE
    existing_date
  END
);

-- Return the source ping that reported the first not null value
CREATE TEMP FUNCTION get_source_ping(
  existing_source STRING,
  existing_date DATE,
  first_session_value STRING,
  first_session_date DATE,
  metrics_value STRING,
  metrics_date DATE
) AS (
  CASE
  WHEN
    existing_source IS NULL
    AND first_session_value IS NOT NULL
  THEN
    "first_session"
  WHEN
    existing_source IS NULL
    AND first_session_value IS NULL
    AND metrics_value IS NOT NULL
  THEN
    "metrics"
  WHEN
    existing_source IS NOT NULL
    AND first_session_value IS NOT NULL
    AND first_session_date < existing_date
  THEN
    "first_session"
  WHEN
    existing_source IS NOT NULL
    AND first_session_value IS NULL
    AND metrics_value IS NOT NULL
    AND metrics_date < existing_date
  THEN
    "metrics"
  ELSE
    existing_source
  END
);

WITH first_seen AS (
  SELECT
    client_id,
    first_seen_date
  FROM
    `mozdata.org_mozilla_firefox.baseline_clients_first_seen`
  WHERE
    submission_date = @submission_date
    AND first_seen_date = submission_date
),
existing AS (
  SELECT
    client_id,
    first_seen_date,
    first_run_date,
    first_reported_country,
    first_reported_isp,
    channel,
    device_manufacturer,
    device_model,
    os_version,
    adjust_campaign,
    adjust_ad_group,
    adjust_creative,
    adjust_network,
    install_source,
    metadata.reported_first_session_ping,
    metadata.reported_metrics_ping,
    metadata.min_first_session_ping_date,
    metadata.adjust_network__source_ping,
    metadata.install_source__source_ping,
    metadata.adjust_network__source_ping_date,
    metadata.install_source__source_ping_date
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.firefox_android_clients_v1`
    --Q/ In the scenario where a client is deleted and its first seen date is reprocessed --Do we want to keep history(current behavior) or delete too (filter out and load data agin for that submission_date). Consider changes in numbers and that clients deleted in days that are not reprocessed remain)
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
    DATE(submission_timestamp) = @submission_date
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
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
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
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
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
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
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
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
  UNION ALL
  -- Fenix Release
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
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.metrics_install_source IS NOT NULL
    AND metrics.string.metrics_adjust_network IS NOT NULL
  GROUP BY
    client_id
),
-- Guarantee the min value in case a client_id appears for different channels - Validate if this scenario is possible
metrics_ping_unique AS (
  SELECT
    client_id,
    DATE(MIN(min_submission_timestamp)) AS metrics_ping_submission_date,
    ANY_VALUE(adjust_network) AS adjust_network,
    ANY_VALUE(install_source) AS install_source
  FROM
    metrics_ping
  GROUP BY
    client_id
),
existing_with_pings_new_data AS (
  SELECT
    existing.client_id,
    existing.first_seen_date,
    COALESCE(existing.first_run_date, first_session.first_run_date) AS first_run_date,
    COALESCE(
      existing.first_reported_country,
      first_session.first_reported_country
    ) AS first_reported_country,
    COALESCE(existing.first_reported_isp, first_session.first_reported_isp) AS first_reported_isp,
    COALESCE(existing.channel, first_session.channel) AS channel,
    COALESCE(
      existing.device_manufacturer,
      first_session.device_manufacturer
    ) AS device_manufacturer,
    COALESCE(existing.device_model, first_session.device_model) AS device_model,
    COALESCE(existing.os_version, first_session.os_version) AS os_version,
    COALESCE(existing.adjust_campaign, first_session.adjust_campaign) AS adjust_campaign,
    COALESCE(existing.adjust_ad_group, first_session.adjust_ad_group) AS adjust_ad_group,
    COALESCE(existing.adjust_creative, first_session.adjust_creative) AS adjust_creative,
    get_first_not_null_value(
      existing.adjust_network,
      existing.adjust_network__source_ping_date,
      first_session.adjust_network,
      first_session.first_run_date,
      metrics.adjust_network,
      metrics.metrics_ping_submission_date
    ) AS adjust_network,
    get_first_not_null_value(
      existing.install_source,
      existing.install_source__source_ping_date,
      NULL,
      NULL,
      metrics.install_source,
      metrics.metrics_ping_submission_date
    ) AS install_source,
    STRUCT(
      CASE
      WHEN
        existing.reported_first_session_ping
        OR first_session.client_id IS NOT NULL
      THEN
        TRUE
      ELSE
        FALSE
      END
      AS reported_first_session_ping,
      CASE
      WHEN
        existing.reported_metrics_ping
        OR metrics.client_id IS NOT NULL
      THEN
        TRUE
      ELSE
        FALSE
      END
      AS reported_metrics_ping,
      LEAST(existing.first_run_date, first_session.first_run_date) AS min_first_session_ping_date,
      get_source_ping(
        existing.adjust_network__source_ping,
        existing.adjust_network__source_ping_date,
        first_session.adjust_network,
        first_session.first_run_date,
        metrics.adjust_network,
        metrics.metrics_ping_submission_date
      ) AS adjust_network__source_ping,
      'metrics' AS install_source__source_ping,
      get_first_not_null_date(
        existing.adjust_network,
        existing.adjust_network__source_ping_date,
        first_session.adjust_network,
        first_session.first_run_date,
        metrics.adjust_network,
        metrics.metrics_ping_submission_date
      ) AS adjust_network__source_ping_date,
      get_first_not_null_date(
        existing.install_source,
        existing.install_source__source_ping_date,
        NULL,
        NULL,
        metrics.install_source,
        metrics.metrics_ping_submission_date
      ) AS install_source__source_ping_date
    ) AS metadata
  FROM
    existing
  LEFT JOIN
    first_session_ping first_session
  USING
    (client_id)
  LEFT JOIN
    metrics_ping_unique AS metrics
  USING
    (client_id)
),
first_seen_with_pings_new_data AS (
  SELECT
    first_seen.client_id,
    first_seen.first_seen_date,
    first_session.first_run_date AS first_run_date,
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
        metrics.install_source IS NOT NULL
      THEN
        metrics.metrics_ping_submission_date
      ELSE
        NULL
      END
      AS install_source__source_ping_date
    ) AS metadata
  FROM
    first_seen
  LEFT JOIN
    first_session_ping first_session
  USING
    (client_id)
  LEFT JOIN
    metrics_ping_unique AS metrics
  USING
    (client_id)
)
SELECT
  *
FROM
  existing_with_pings_new_data
UNION ALL
SELECT
  first_seen_with_pings_new_data.*
FROM
  first_seen_with_pings_new_data
LEFT JOIN
  existing_with_pings_new_data
USING
  (client_id)
WHERE
  existing_with_pings_new_data.client_id IS NULL
