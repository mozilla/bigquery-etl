-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.bounce_tracking_protection`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_firefox", '21850000').channel AS normalized_channel,
  additional_properties,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.bounce_tracking_protection`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", '21850000').channel AS normalized_channel,
  additional_properties,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.bounce_tracking_protection`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fenix", '21850000').channel AS normalized_channel,
  additional_properties,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.bounce_tracking_protection`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fenix_nightly", '21850000').channel AS normalized_channel,
  additional_properties,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.bounce_tracking_protection`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
  mozfun.norm.fenix_app_info("org_mozilla_fennec_aurora", '21850000').channel AS normalized_channel,
  additional_properties,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.bounce_tracking_protection`