-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.topsites_impression`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.top_site_contile_tile_id,
      metrics.quantity.top_sites_contile_tile_id
    ) AS `quantity`,
    STRUCT(
      metrics.string.top_site_contile_advertiser,
      metrics.string.top_sites_contile_advertiser,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.url2.top_site_contile_reporting_url,
      metrics.url2.top_sites_contile_reporting_url
    ) AS `url2`,
    STRUCT(metrics.uuid.top_site_context_id, metrics.uuid.top_sites_context_id) AS `uuid`,
    STRUCT(
      metrics.url.top_site_contile_reporting_url,
      metrics.url.top_sites_contile_reporting_url
    ) AS `url`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.topsites_impression`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.top_site_contile_tile_id,
      metrics.quantity.top_sites_contile_tile_id
    ) AS `quantity`,
    STRUCT(
      metrics.string.top_site_contile_advertiser,
      metrics.string.top_sites_contile_advertiser,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.url2.top_site_contile_reporting_url,
      metrics.url2.top_sites_contile_reporting_url
    ) AS `url2`,
    STRUCT(metrics.uuid.top_site_context_id, metrics.uuid.top_sites_context_id) AS `uuid`,
    STRUCT(
      metrics.url.top_site_contile_reporting_url,
      metrics.url.top_sites_contile_reporting_url
    ) AS `url`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.topsites_impression`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.top_site_contile_tile_id,
      metrics.quantity.top_sites_contile_tile_id
    ) AS `quantity`,
    STRUCT(
      metrics.string.top_site_contile_advertiser,
      metrics.string.top_sites_contile_advertiser,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.url2.top_site_contile_reporting_url,
      metrics.url2.top_sites_contile_reporting_url
    ) AS `url2`,
    STRUCT(metrics.uuid.top_site_context_id, metrics.uuid.top_sites_context_id) AS `uuid`,
    STRUCT(
      metrics.url.top_site_contile_reporting_url,
      metrics.url.top_sites_contile_reporting_url
    ) AS `url`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.topsites_impression`
