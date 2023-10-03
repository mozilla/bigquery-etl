-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.deletion_request`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
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
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.mozillavpn.deletion_request`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  "release" AS normalized_channel,
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
  metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.deletion_request`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
  "release" AS normalized_channel,
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
              STRUCT(experiments.value.extra.type, experiments.value.extra.enrollment_id) AS extra
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.deletion_request`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
  "release" AS normalized_channel,
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
              STRUCT(experiments.value.extra.type, experiments.value.extra.enrollment_id) AS extra
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.deletion_request`
