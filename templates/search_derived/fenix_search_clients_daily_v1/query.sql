-- Older versions separate source and engine with an underscore instead of period
CREATE TEMP FUNCTION normalize_search_key(key STRING) AS ((
  WITH underscore_replaced AS (
    SELECT IF(ARRAY_LENGTH(SPLIT(key, '_')) = 2, REPLACE(key, '_', '.'), key) AS key
  )
  SELECT
    IF(ARRAY_LENGTH(SPLIT(underscore_replaced.key, '.')) = 2, underscore_replaced.key, NULL)
  FROM
    underscore_replaced
));

-- baseline has locale but not default search engine, metrics has default search engine but not locale
WITH client_locales AS (
  SELECT
    client_info.client_id,
    udf_mode_last(ARRAY_AGG(metrics.string.glean_baseline_locale)) AS locale
  FROM
    org_mozilla_fenix_stable.baseline_v1
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_info.client_id
),
flattened_searches AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    UNIX_DATE(udf_iso8601_str_to_date(client_info.first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(
      udf_iso8601_str_to_date(ping_info.end_time),
      udf_iso8601_str_to_date(client_info.first_run_date),
      DAY
    ) AS profile_age_in_days,
    SPLIT(normalize_search_key(searches.key), '.')[SAFE_OFFSET(0)] AS engine,
    SPLIT(normalize_search_key(searches.key), '.')[SAFE_OFFSET(1)] AS source,
    IF(normalize_search_key(searches.key) IS NULL, 0, searches.value) AS search_count -- TODO: Normalize before
  FROM
    org_mozilla_fenix_stable.metrics_v1
  LEFT JOIN
    client_locales ON client_id = client_info.client_id
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(metrics.labeled_counter.metrics_search_count) = 0,
         [STRUCT<key STRING, value INT64>(NULL, 0)],
         metrics.labeled_counter.metrics_search_count
      )) AS searches
)

SELECT
  submission_date,
  client_info.client_id AS client_id,
  engine,
  source,
  SUM(search_count) AS search_count,
  udf_mode_last(ARRAY_AGG(normalized_country_code)) AS country,
  udf_mode_last(ARRAY_AGG(locale)) AS locale,
  'Fenix' AS app_name,  -- normalized_app_name is always null
  udf_mode_last(ARRAY_AGG(client_info.app_display_version)) AS app_version,
  udf_mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  udf_mode_last(ARRAY_AGG(normalized_os)) AS os,
  udf_mode_last(ARRAY_AGG(client_info.android_sdk_version)) AS os_version,  -- Use sdk version to match core pings
  udf_mode_last(ARRAY_AGG(metrics.string.search_default_engine_code)) AS default_search_engine,
  udf_mode_last(ARRAY_AGG(metrics.string.search_default_engine_submission_url)) AS default_search_engine_submission_url,
  udf_mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
  udf_mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
  CAST(NULL AS STRING) AS distribution_id,
  udf_mode_last(ARRAY_AGG(sample_id)) AS sample_id
FROM
  flattened_searches
WHERE
  submission_date = @submission_date
GROUP BY
  client_id,
  submission_date,
  engine,
  source
