-- Hourly aggregate of ads client metrics pings across iOS and Android platforms.
-- Covers operation counts (labeled_counter) and error counts (labeled_string)
-- broken down by key, plus total ping count and device dimensions.
-- Source: org_mozilla_ios_firefox_stable, org_mozilla_firefox_stable,
--         org_mozilla_fenix_stable metrics pings.
-- Only pings with at least one ads_client metric entry are included.
WITH base AS (
  SELECT
    'ios' AS platform,
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
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
  UNION ALL
  SELECT
    'android' AS platform,
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
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
  UNION ALL
  SELECT
    'android-nightly' AS platform,
    submission_timestamp,
    normalized_country_code,
    client_info.app_display_version AS app_version,
    normalized_channel,
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
)
SELECT
  @submission_date AS submission_date,
  TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
  platform,
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
  -- Client errors (labeled_string: count pings that have an entry for each key)
  COUNTIF(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'new') > 0
  ) AS client_error_new_pings,
  COUNTIF(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'request_ads') > 0
  ) AS client_error_request_ads_pings,
  COUNTIF(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'record_click') > 0
  ) AS client_error_record_click_pings,
  COUNTIF(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'record_impression') > 0
  ) AS client_error_record_impression_pings,
  COUNTIF(
    (SELECT COUNT(*) FROM UNNEST(client_error) WHERE key = 'report_ad') > 0
  ) AS client_error_report_ad_pings,
  -- Other error types (count pings with any entry for that metric)
  COUNTIF(ARRAY_LENGTH(build_cache_error) > 0) AS build_cache_error_pings,
  COUNTIF(ARRAY_LENGTH(deserialization_error) > 0) AS deserialization_error_pings,
  COUNTIF(ARRAY_LENGTH(http_cache_outcome) > 0) AS http_cache_outcome_pings,
FROM
  base
GROUP BY
  submission_date,
  submission_hour,
  platform,
  normalized_country_code,
  app_version,
  normalized_channel
