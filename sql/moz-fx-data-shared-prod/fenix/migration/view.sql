-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.migration`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  submission_date,
  additional_properties,
  client_info,
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
      CAST(NULL AS TIMESTAMP) AS `parsed_date`,
      CAST(NULL AS ARRAY<STRING>) AS `parsed_x_source_tags`,
      CAST(NULL AS STRUCT<`tls_version` STRING, `tls_cipher_hex` STRING>) AS `parsed_x_lb_tags`
    ) AS `header`,
    metadata.user_agent,
    metadata.isp
  ) AS `metadata`,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      metrics.datetime.migration_telemetry_identifiers_fennec_profile_creation_date,
      CAST(NULL AS STRING) AS `raw_migration_telemetry_identifiers_fennec_profile_creation_date`
    ) AS `datetime`,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.url,
    metrics.text
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    ping_info.end_time,
    ping_info.experiments,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    CAST(NULL AS TIMESTAMP) AS `parsed_start_time`,
    CAST(NULL AS TIMESTAMP) AS `parsed_end_time`
  ) AS `ping_info`,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
  ) AS `metrics`,
  normalized_app_name,
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
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
  ) AS `metrics`,
  normalized_app_name,
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
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
  ) AS `metrics`,
  normalized_app_name,
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
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    metrics.labeled_counter,
    metrics.labeled_string,
    metrics.string,
    metrics.timespan,
    metrics.uuid,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.migration`
