-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.vpnsession`
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
  `moz-fx-data-shared-prod.mozillavpn.vpnsession`
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
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.vpnsession`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.vpnsession`
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
    metrics.quantity,
    STRUCT(
      metrics.string.session_dns_type,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS string,
    STRUCT(metrics.uuid.session_session_id, metrics.uuid.session_installation_id) AS uuid,
    metrics.counter
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.vpnsession`
