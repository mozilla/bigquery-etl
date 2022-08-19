-- Override the default glean_usage generated view to union data from all VPN clients.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.deletion_request`
AS
-- Data from VPN clients using Glean.js
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozillavpn.deletion_request`
UNION ALL
-- Data from VPN Android clients using Glean Kotlin SDK
SELECT
  * REPLACE (
    STRUCT(
      client_info.android_sdk_version AS android_sdk_version,
      client_info.app_build AS app_build,
      client_info.app_channel AS app_channel,
      client_info.app_display_version AS app_display_version,
      client_info.architecture AS architecture,
      client_info.client_id AS client_id,
      client_info.device_manufacturer AS device_manufacturer,
      client_info.device_model AS device_model,
      client_info.first_run_date AS first_run_date,
      client_info.locale AS locale,
      client_info.os AS os,
      client_info.os_version AS os_version,
      client_info.telemetry_sdk_build AS telemetry_sdk_build,
      client_info.build_date AS build_date
    ) AS client_info,
    (
      SELECT AS STRUCT
        metadata.* REPLACE (
          STRUCT(
            metadata.header.`date` AS `date`,
            metadata.header.dnt AS dnt,
            metadata.header.x_debug_id AS x_debug_id,
            metadata.header.x_pingsender_version AS x_pingsender_version,
            metadata.header.x_source_tags AS x_source_tags,
            metadata.header.x_telemetry_agent AS x_telemetry_agent,
            metadata.header.x_foxsec_ip_reputation AS x_foxsec_ip_reputation,
            metadata.header.x_lb_tags AS x_lb_tags,
            metadata.header.parsed_date AS parsed_date,
            metadata.header.parsed_x_source_tags AS parsed_x_source_tags,
            metadata.header.parsed_x_lb_tags AS parsed_x_lb_tags
          ) AS header
        )
    ) AS metadata,
    STRUCT(
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS jwe,
      metrics.labeled_counter AS labeled_counter,
      CAST(
        NULL
        AS
          ARRAY<
            STRUCT<
              key STRING,
              value ARRAY<STRUCT<key STRING, value STRUCT<denominator INT64, numerator INT64>>>
            >
          >
      ) AS labeled_rate,
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS url,
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS text
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.deletion_request`
