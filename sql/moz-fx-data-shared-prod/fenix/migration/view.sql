-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.migration`
AS
SELECT
  submission_date,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
UNION ALL
SELECT
  NULL AS submission_date,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      NULL AS migration_telemetry_identifiers_fennec_profile_creation_date,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS datetime,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.url,
    metrics.text
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.migration`
UNION ALL
SELECT
  NULL AS submission_date,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      NULL AS migration_telemetry_identifiers_fennec_profile_creation_date,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS datetime,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.url,
    metrics.text
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.migration`
UNION ALL
SELECT
  NULL AS submission_date,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      NULL AS migration_telemetry_identifiers_fennec_profile_creation_date,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS datetime,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.url,
    metrics.text
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.migration`
UNION ALL
SELECT
  NULL AS submission_date,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      NULL AS migration_telemetry_identifiers_fennec_profile_creation_date,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS datetime,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.url,
    metrics.text
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.migration`
