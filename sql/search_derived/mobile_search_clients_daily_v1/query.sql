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
-- baseline for org_mozilla_fenix (Firefox Preview beta)
baseline_org_mozilla_fenix AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fenix.baseline
),
-- baseline for org_mozilla_fenix (Firefox Preview beta)
metrics_org_mozilla_fenix AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    'Firefox Preview' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version,
    'beta' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
),
-- baseline for org_mozilla_fenix_nightly (Firefox Preview nightly)
baseline_org_mozilla_fenix_nightly AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fenix_nightly.baseline
),
-- baseline for org_mozilla_fenix_nightly (Firefox Preview nightly)
metrics_org_mozilla_fenix_nightly AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    'Firefox Preview' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version,
    'nightly' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    org_mozilla_fenix_nightly.metrics AS org_mozilla_fenix_nightly_metrics
),
-- baseline for org_mozilla_fennec_aurora (Fenix nightly)
baseline_org_mozilla_fennec_aurora AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fennec_aurora.baseline
),
-- baseline for org_mozilla_fennec_aurora (Fenix nightly)
metrics_org_mozilla_fennec_aurora AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version,
    'nightly' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
),
-- baseline for org_mozilla_firefox_beta (Fenix beta)
baseline_org_mozilla_firefox_beta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_firefox_beta.baseline
),
-- baseline for org_mozilla_firefox_beta (Fenix beta)
metrics_org_mozilla_firefox_beta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version,
    'beta' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
),
-- baseline for org_mozilla_firefox (Fenix release)
baseline_org_mozilla_firefox AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_firefox.baseline
),
-- baseline for org_mozilla_firefox (Fenix release)
metrics_org_mozilla_firefox AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version,
    'release' AS channel,
    normalized_os,
    client_info.android_sdk_version,
    metrics.string.search_default_engine_code,
    metrics.string.search_default_engine_submission_url,
    sample_id,
    CAST(NULL AS STRING) AS distribution_id,
    metrics.labeled_counter.metrics_search_count,
    client_info.first_run_date,
    ping_info.end_time
  FROM
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
),
fenix_baseline AS (
  SELECT
    *
  FROM
    baseline_org_mozilla_fenix
  UNION ALL
  SELECT
    *
  FROM
    baseline_org_mozilla_fenix_nightly
  UNION ALL
  SELECT
    *
  FROM
    baseline_org_mozilla_fennec_aurora
  UNION ALL
  SELECT
    *
  FROM
    baseline_org_mozilla_firefox_beta
  UNION ALL
  SELECT
    *
  FROM
    baseline_org_mozilla_firefox
),
fenix_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_fenix
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_fenix_nightly
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_fennec_aurora
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_firefox_beta
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_firefox
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
