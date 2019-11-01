-- Older versions have source.engine instead of engine.source
-- Assume action is one of actionbar, listitem, suggestion, or quicksearch
-- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/data/core-ping.html#searches
CREATE TEMP FUNCTION normalize_search_key(key STRING) AS (
  CASE
    WHEN ARRAY_LENGTH(SPLIT(key, '.')) != 2 THEN []
    WHEN SPLIT(key, '.')[OFFSET(0)] IN UNNEST(['actionbar', 'listitem', 'suggestion', 'quicksearch'])
      THEN ARRAY_REVERSE(SPLIT(key, '.'))
    ELSE SPLIT(key, '.')
    END
);

WITH flattened_searches AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    SAFE_SUBTRACT(UNIX_DATE(DATE(submission_timestamp)), profile_date) AS profile_age_in_days,
    normalize_search_key(searches.key)[SAFE_OFFSET(0)] AS engine,
    normalize_search_key(searches.key)[SAFE_OFFSET(1)] AS source,
    IF(ARRAY_LENGTH(normalize_search_key(searches.key)) = 0, 0, searches.value) AS search_count  -- TODO: Normalize before
  FROM
    telemetry.core
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(searches) = 0,
         [STRUCT<key STRING, value INT64>(NULL, 0)],
         searches
      )) AS searches
)

SELECT
  submission_date,
  client_id,
  engine,
  source,
  SUM(search_count) AS search_count,
  udf_mode_last(ARRAY_AGG(normalized_country_code)) AS country,
  udf_mode_last(ARRAY_AGG(locale)) AS locale,
  udf_mode_last(ARRAY_AGG(normalized_app_name)) AS app_name,
  udf_mode_last(ARRAY_AGG(metadata.uri.app_version)) AS app_version,
  udf_mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  udf_mode_last(ARRAY_AGG(normalized_os)) AS os,
  udf_mode_last(ARRAY_AGG(normalized_os_version)) AS os_version,
  udf_mode_last(ARRAY_AGG(default_search)) AS default_search_engine,
  CAST(NULL AS STRING) AS default_search_engine_submission_url,
  udf_mode_last(ARRAY_AGG(profile_date)) AS profile_creation_date,
  udf_mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
  udf_mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
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
