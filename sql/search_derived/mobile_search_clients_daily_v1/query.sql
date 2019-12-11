CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));
CREATE TEMP FUNCTION udf_parse_iso8601_date(date_str STRING) RETURNS DATE AS (
  COALESCE(
    SAFE.PARSE_DATE('%F', SAFE.SUBSTR(date_str, 0, 10)),
    SAFE.PARSE_DATE('%Y%m%d', SAFE.SUBSTR(date_str, 0, 8))
  )
);
--
-- Older versions separate source and engine with an underscore instead of period
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_fenix_search_key(key STRING) AS ((
  CASE
    WHEN ARRAY_LENGTH(SPLIT(key, '_')) = 2 THEN SPLIT(key, '_')
    WHEN ARRAY_LENGTH(SPLIT(key, '.')) = 2 THEN SPLIT(key, '.')
    ELSE []
  END
));

-- Older versions have source.engine instead of engine.source
-- Assume action is one of actionbar, listitem, suggestion, or quicksearch
-- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/data/core-ping.html#searches
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_core_search_key(key STRING) AS (
  CASE
    WHEN ARRAY_LENGTH(SPLIT(key, '.')) != 2 THEN []
    WHEN SPLIT(key, '.')[OFFSET(0)] IN UNNEST(['actionbar', 'listitem', 'suggestion', 'quicksearch'])
      THEN ARRAY_REVERSE(SPLIT(key, '.'))
    ELSE SPLIT(key, '.')
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
      IF(ARRAY_LENGTH(searches) = 0,
         null_search(),
         searches
      )) AS searches
),

-- baseline has locale but not default search engine, metrics has default search engine but not locale
fenix_client_locales AS (
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

fenix_flattened_searches AS (
  SELECT
    *,
    normalize_fenix_search_key(searches.key) AS normalized_search_key,
    searches.value AS search_count,
    UNIX_DATE(udf_parse_iso8601_date(client_info.first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(
      udf_parse_iso8601_date(ping_info.end_time),
      udf_parse_iso8601_date(client_info.first_run_date),
      DAY
    ) AS profile_age_in_days
  FROM
    org_mozilla_fenix_stable.metrics_v1
  LEFT JOIN
    fenix_client_locales ON client_id = client_info.client_id
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(metrics.labeled_counter.metrics_search_count) = 0,
         null_search(),
         metrics.labeled_counter.metrics_search_count
      )) AS searches
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
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    normalized_search_key,
    search_count,
    normalized_country_code AS country,
    locale,
    'Fenix' AS app_name,
    client_info.app_display_version AS app_version,
    normalized_channel AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    CAST(NULL AS STRING) AS distribution_id,
    profile_creation_date,
    profile_age_in_days,
    sample_id
  FROM
    fenix_flattened_searches
)

SELECT
  submission_date,
  client_id,
  IF(search_count > 10000, NULL, normalized_search_key[SAFE_OFFSET(0)]) AS engine,
  IF(search_count > 10000, NULL, normalized_search_key[SAFE_OFFSET(1)]) AS source,
  app_name,
  SUM(
    IF(
      ARRAY_LENGTH(normalized_search_key) = 0
      OR search_count > 10000,
      0,
      search_count
    )
  ) AS search_count,
  udf_mode_last(ARRAY_AGG(country)) AS country,
  udf_mode_last(ARRAY_AGG(locale)) AS locale,
  udf_mode_last(ARRAY_AGG(app_version)) AS app_version,
  udf_mode_last(ARRAY_AGG(channel)) AS channel,
  udf_mode_last(ARRAY_AGG(os)) AS os,
  udf_mode_last(ARRAY_AGG(os_version)) AS os_version,
  udf_mode_last(ARRAY_AGG(default_search_engine)) AS default_search_engine,
  udf_mode_last(ARRAY_AGG(default_search_engine_submission_url)) AS default_search_engine_submission_url,
  udf_mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
  udf_mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
  udf_mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
  udf_mode_last(ARRAY_AGG(sample_id)) AS sample_id
FROM
  combined_search_clients
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  client_id,
  engine,
  source,
  app_name
