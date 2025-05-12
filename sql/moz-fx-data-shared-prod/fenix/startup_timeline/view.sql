-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.startup_timeline`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.startup_timeline_framework_start_error,
      metrics.boolean.startup_timeline_framework_start_read_error
    ) AS `boolean`,
    STRUCT(metrics.counter.startup_timeline_clock_ticks_per_second) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.timespan.startup_timeline_framework_start,
      metrics.timespan.startup_timeline_framework_primary,
      metrics.timespan.startup_timeline_framework_secondary
    ) AS `timespan`,
    STRUCT(metrics.quantity.startup_timeline_clock_ticks_per_second_v2) AS `quantity`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.startup_timeline`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.startup_timeline_framework_start_error,
      metrics.boolean.startup_timeline_framework_start_read_error
    ) AS `boolean`,
    STRUCT(metrics.counter.startup_timeline_clock_ticks_per_second) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.timespan.startup_timeline_framework_start,
      metrics.timespan.startup_timeline_framework_primary,
      metrics.timespan.startup_timeline_framework_secondary
    ) AS `timespan`,
    STRUCT(metrics.quantity.startup_timeline_clock_ticks_per_second_v2) AS `quantity`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.startup_timeline`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.startup_timeline_framework_start_error,
      metrics.boolean.startup_timeline_framework_start_read_error
    ) AS `boolean`,
    STRUCT(metrics.counter.startup_timeline_clock_ticks_per_second) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.timespan.startup_timeline_framework_start,
      metrics.timespan.startup_timeline_framework_primary,
      metrics.timespan.startup_timeline_framework_secondary
    ) AS `timespan`,
    STRUCT(metrics.quantity.startup_timeline_clock_ticks_per_second_v2) AS `quantity`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.startup_timeline`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.startup_timeline_framework_start_error,
      metrics.boolean.startup_timeline_framework_start_read_error
    ) AS `boolean`,
    STRUCT(metrics.counter.startup_timeline_clock_ticks_per_second) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.timespan.startup_timeline_framework_start,
      metrics.timespan.startup_timeline_framework_primary,
      metrics.timespan.startup_timeline_framework_secondary
    ) AS `timespan`,
    STRUCT(metrics.quantity.startup_timeline_clock_ticks_per_second_v2) AS `quantity`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.startup_timeline`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.startup_timeline_framework_start_error,
      metrics.boolean.startup_timeline_framework_start_read_error
    ) AS `boolean`,
    STRUCT(metrics.counter.startup_timeline_clock_ticks_per_second) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.timespan.startup_timeline_framework_start,
      metrics.timespan.startup_timeline_framework_primary,
      metrics.timespan.startup_timeline_framework_secondary
    ) AS `timespan`,
    STRUCT(metrics.quantity.startup_timeline_clock_ticks_per_second_v2) AS `quantity`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.startup_timeline`
