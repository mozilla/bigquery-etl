-- Hourly aggregate of ads client metrics pings across iOS and Android platforms.
-- Covers operation counts (labeled_counter) and error occurrence counts (labeled_string)
-- broken down by key, plus total ping count and device dimensions.
-- Source tables and their channels:
--   - org_mozilla_ios_firefox_stable (iOS release)
--   - org_mozilla_firefox_stable (Android release)
--   - org_mozilla_fenix_stable (Android nightly)
-- Only pings with at least one ads_client metric entry are included.
WITH ios_base AS (
  SELECT
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
    'Mobile' AS surface,
    client_info.os AS normalized_os,
    'Firefox for iOS' AS app_name,
    normalized_channel AS channel,
    metrics.labeled_counter.ads_client_client_operation_total AS client_operation_total,
    metrics.labeled_string.ads_client_client_error AS client_error,
    metrics.labeled_string.ads_client_build_cache_error AS build_cache_error,
    metrics.labeled_string.ads_client_deserialization_error AS deserialization_error,
    metrics.labeled_string.ads_client_http_cache_outcome AS http_cache_outcome,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND (
      ARRAY_LENGTH(metrics.labeled_counter.ads_client_client_operation_total) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_client_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_build_cache_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_deserialization_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_http_cache_outcome) > 0
    )
),
android_release_base AS (
  SELECT
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
    'Mobile' AS surface,
    client_info.os AS normalized_os,
    'Fenix' AS app_name,
    normalized_channel AS channel,
    metrics.labeled_counter.ads_client_client_operation_total AS client_operation_total,
    metrics.labeled_string.ads_client_client_error AS client_error,
    metrics.labeled_string.ads_client_build_cache_error AS build_cache_error,
    metrics.labeled_string.ads_client_deserialization_error AS deserialization_error,
    metrics.labeled_string.ads_client_http_cache_outcome AS http_cache_outcome,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND (
      ARRAY_LENGTH(metrics.labeled_counter.ads_client_client_operation_total) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_client_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_build_cache_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_deserialization_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_http_cache_outcome) > 0
    )
),
android_nightly_base AS (
  SELECT
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
    'Mobile' AS surface,
    client_info.os AS normalized_os,
    'Fenix' AS app_name,
    normalized_channel AS channel,
    metrics.labeled_counter.ads_client_client_operation_total AS client_operation_total,
    metrics.labeled_string.ads_client_client_error AS client_error,
    metrics.labeled_string.ads_client_build_cache_error AS build_cache_error,
    metrics.labeled_string.ads_client_deserialization_error AS deserialization_error,
    metrics.labeled_string.ads_client_http_cache_outcome AS http_cache_outcome,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND (
      ARRAY_LENGTH(metrics.labeled_counter.ads_client_client_operation_total) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_client_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_build_cache_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_deserialization_error) > 0
      OR ARRAY_LENGTH(metrics.labeled_string.ads_client_http_cache_outcome) > 0
    )
),
base AS (
  SELECT
    *
  FROM
    ios_base
  UNION ALL
  SELECT
    *
  FROM
    android_release_base
  UNION ALL
  SELECT
    *
  FROM
    android_nightly_base
)
SELECT
  @submission_date AS submission_date,
  TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
  surface,
  normalized_os,
  app_name,
  channel,
  normalized_country_code,
  app_version,
  normalized_channel,
  COUNT(*) AS ping_count,
  -- Operation totals (labeled_counter: sum values per key)
  SUM(
    IFNULL((SELECT SUM(value) FROM UNNEST(client_operation_total) WHERE key = 'new'), 0)
  ) AS op_new,
  SUM(
    IFNULL((SELECT SUM(value) FROM UNNEST(client_operation_total) WHERE key = 'request_ads'), 0)
  ) AS op_request_ads,
  SUM(
    IFNULL((SELECT SUM(value) FROM UNNEST(client_operation_total) WHERE key = 'record_click'), 0)
  ) AS op_record_click,
  SUM(
    IFNULL(
      (SELECT SUM(value) FROM UNNEST(client_operation_total) WHERE key = 'record_impression'),
      0
    )
  ) AS op_record_impression,
  SUM(
    IFNULL((SELECT SUM(value) FROM UNNEST(client_operation_total) WHERE key = 'report_ad'), 0)
  ) AS op_report_ad,
  -- Client errors (labeled_string: sum total occurrences per key)
  SUM((SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'new')) AS client_error_new,
  SUM(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'request_ads')
  ) AS client_error_request_ads,
  SUM(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'record_click')
  ) AS client_error_record_click,
  SUM(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'record_impression')
  ) AS client_error_record_impression,
  SUM(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'report_ad')
  ) AS client_error_report_ad,
  -- Other error types (sum total occurrences for that metric)
  SUM(ARRAY_LENGTH(build_cache_error)) AS build_cache_error,
  SUM(ARRAY_LENGTH(deserialization_error)) AS deserialization_error,
  SUM(ARRAY_LENGTH(http_cache_outcome)) AS http_cache_outcome,
FROM
  base
GROUP BY
  submission_date,
  submission_hour,
  surface,
  normalized_os,
  app_name,
  channel,
  normalized_country_code,
  app_version,
  normalized_channel
