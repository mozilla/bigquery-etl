-- Older versions separate source and engine with an underscore instead of period
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_fenix_search_key(key STRING) AS (
  (
    CASE
    WHEN
      ARRAY_LENGTH(SPLIT(key, '_')) = 2
    THEN
      SPLIT(key, '_')
    WHEN
      ARRAY_LENGTH(SPLIT(key, '.')) = 2
    THEN
      SPLIT(key, '.')
    ELSE
      []
    END
  )
);

-- Older versions have source.engine instead of engine.source
-- Assume action is one of actionbar, listitem, suggestion, or quicksearch
-- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/data/core-ping.html#searches
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_core_search_key(key STRING) AS (
  CASE
  WHEN
    ARRAY_LENGTH(SPLIT(key, '.')) != 2
  THEN
    []
  WHEN
    SPLIT(key, '.')[OFFSET(0)] IN UNNEST(['actionbar', 'listitem', 'suggestion', 'quicksearch'])
  THEN
    ARRAY_REVERSE(SPLIT(key, '.'))
  ELSE
    SPLIT(key, '.')
  END
);

CREATE TEMP FUNCTION null_search() AS (
  [STRUCT<key STRING, value INT64>(NULL, 0)]
);

WITH core_flattened_searches AS (
  SELECT
    *,
    normalize_core_search_key(searches.key) AS normalized_search_key,
    searches.value AS search_count,
    SAFE_SUBTRACT(UNIX_DATE(DATE(submission_timestamp)), profile_date) AS profile_age_in_days
  FROM
    telemetry.core
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(searches) = 0, null_search(), searches)
    ) AS searches
),
{baseline_and_metrics_by_namespace } fenix_baseline AS (
  {fenix_baseline }
),
fenix_metrics AS (
  {fenix_metrics }
),
--  older fenix clients don't send locale in the metrics ping
fenix_client_locales AS (
  SELECT
    client_id,
    udf.mode_last(ARRAY_AGG(locale)) AS locale
  FROM
    fenix_baseline
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
fenix_flattened_searches AS (
  SELECT
    *,
    normalize_fenix_search_key(searches.key) AS normalized_search_key,
    searches.value AS search_count,
    UNIX_DATE(udf.parse_iso8601_date(first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(
      udf.parse_iso8601_date(end_time),
      udf.parse_iso8601_date(first_run_date),
      DAY
    ) AS profile_age_in_days
  FROM
    fenix_metrics
  LEFT JOIN
    fenix_client_locales
  USING
    (client_id)
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(metrics_search_count) = 0, null_search(), metrics_search_count)
    ) AS searches
),
combined_search_clients AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    normalized_search_key,
    search_count,
    normalized_country_code AS country,
    locale,
    normalized_app_name AS app_name,
    normalized_app_name,
    metadata.uri.app_version AS app_version,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    default_search AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    distribution_id,
    profile_date AS profile_creation_date,
    profile_age_in_days,
    sample_id
  FROM
    core_flattened_searches
  UNION ALL
  SELECT
    submission_date,
    client_id,
    normalized_search_key,
    search_count,
    normalized_country_code AS country,
    locale,
    app_name,
    normalized_app_name,
    app_display_version AS app_version,
    channel,
    normalized_os AS os,
    android_sdk_version AS os_version,
    search_default_engine_code AS default_search_engine,
    search_default_engine_submission_url AS default_search_engine_submission_url,
    CAST(NULL AS STRING) AS distribution_id,
    profile_creation_date,
    profile_age_in_days,
    sample_id
  FROM
    fenix_flattened_searches
),
unfiltered_search_clients AS (
  SELECT
    submission_date,
    client_id,
    IF(search_count > 10000, NULL, normalized_search_key[SAFE_OFFSET(0)]) AS engine,
    IF(search_count > 10000, NULL, normalized_search_key[SAFE_OFFSET(1)]) AS source,
    app_name,
    normalized_app_name,
    SUM(
      IF(ARRAY_LENGTH(normalized_search_key) = 0 OR search_count > 10000, 0, search_count)
    ) AS search_count,
    udf.mode_last(ARRAY_AGG(country)) AS country,
    udf.mode_last(ARRAY_AGG(locale)) AS locale,
    udf.mode_last(ARRAY_AGG(app_version)) AS app_version,
    udf.mode_last(ARRAY_AGG(channel)) AS channel,
    udf.mode_last(ARRAY_AGG(os)) AS os,
    udf.mode_last(ARRAY_AGG(os_version)) AS os_version,
    udf.mode_last(ARRAY_AGG(default_search_engine)) AS default_search_engine,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_submission_url)
    ) AS default_search_engine_submission_url,
    udf.mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
    udf.mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
    udf.mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
    udf.mode_last(ARRAY_AGG(sample_id)) AS sample_id
  FROM
    combined_search_clients
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id,
    engine,
    source,
    app_name,
    normalized_app_name
)
SELECT
  *,
  udf.normalize_search_engine(engine) AS normalized_engine
FROM
  unfiltered_search_clients
