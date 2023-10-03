-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.baseline`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.datetime,
    metrics.labeled_counter,
    metrics.string,
    metrics.timespan,
    CAST(NULL AS STRUCT<glean_validation_metrics_ping_count INTEGER>) AS counter
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.mozillavpn.baseline`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    ping_info.end_time,
    (
      SELECT
        ARRAY_AGG(
          STRUCT(
            experiments.key,
            STRUCT(
              experiments.value.branch,
              STRUCT(experiments.value.extra.enrollment_id, experiments.value.extra.type) AS extra
            ) AS value
          )
        )
      FROM
        UNNEST(ping_info.experiments) AS experiments
    ) AS experiments,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.parsed_start_time,
    ping_info.parsed_end_time
  ) AS ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.baseline`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.datetime,
    metrics.labeled_counter,
    metrics.string,
    metrics.timespan,
    CAST(NULL AS STRUCT<glean_validation_metrics_ping_count INTEGER>) AS counter
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.baseline`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.datetime,
    metrics.labeled_counter,
    metrics.string,
    metrics.timespan,
    CAST(NULL AS STRUCT<glean_validation_metrics_ping_count INTEGER>) AS counter
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.baseline`
