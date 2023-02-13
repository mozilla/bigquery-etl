-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.main`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.mozillavpn.main`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.build_date,
    client_info.windows_build_number
  ) AS client_info,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS header,
    metadata.isp,
    metadata.user_agent
  ) AS metadata,
  STRUCT(
    STRUCT(SAFE_CAST(NULL AS STRING) AS key, SAFE_CAST(NULL AS STRING) AS value) AS jwe,
    metrics.labeled_counter,
    STRUCT(
      SAFE_CAST(NULL AS STRING) AS key,
      STRUCT(
        SAFE_CAST(NULL AS STRING) AS key,
        STRUCT(
          SAFE_CAST(NULL AS INTEGER) AS denominator,
          SAFE_CAST(NULL AS INTEGER) AS numerator
        ) AS value
      ) AS value
    ) AS labeled_rate,
    STRUCT(SAFE_CAST(NULL AS STRING) AS key, SAFE_CAST(NULL AS STRING) AS value) AS url,
    STRUCT(SAFE_CAST(NULL AS STRING) AS key, SAFE_CAST(NULL AS STRING) AS value) AS text
  ) AS metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.main`
