-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.usage_reporting`
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
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.metrics_distribution_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.usage_reporting`
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
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.metrics_distribution_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.usage_reporting`
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
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.metrics_distribution_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.usage_reporting`
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
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.metrics_distribution_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.usage_reporting`
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
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.metrics_distribution_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.usage_reporting`
