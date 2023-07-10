-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.events`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.labeled_counter,
    metrics.uuid,
    CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS jwe,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            key STRING,
            value ARRAY<STRUCT<key STRING, value STRUCT<denominator INTEGER, numerator INTEGER>>>
          >
        >
    ) AS labeled_rate,
    CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS text,
    CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS url
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.events`
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.labeled_counter,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.text,
    metrics.url
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.events`
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.labeled_counter,
    metrics.uuid,
    metrics.jwe,
    metrics.labeled_rate,
    metrics.text,
    metrics.url
  ) AS metrics,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.events`
