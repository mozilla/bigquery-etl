-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_fire_tv.metrics`
AS
SELECT
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  metrics
FROM
  `moz-fx-data-shared-prod.org_mozilla_tv_firefox.metrics`
