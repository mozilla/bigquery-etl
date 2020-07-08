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

CREATE TEMP FUNCTION normalize_fenix_experiments(experiments ANY TYPE) AS (
  ARRAY(SELECT AS STRUCT key, value.branch AS value, FROM UNNEST(experiments))
);

CREATE TEMP FUNCTION normalize_core_experiments(experiments ANY TYPE) AS (
  ARRAY(SELECT AS STRUCT key, CAST(NULL AS STRING) AS value, FROM UNNEST(experiments) AS key)
);

-- Add search type value to each element of an array
CREATE TEMP FUNCTION add_search_type(list ANY TYPE, search_type STRING) AS (
  ARRAY(SELECT AS STRUCT *, search_type, FROM UNNEST(list))
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
    metrics.labeled_counter.browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content,
    metrics.labeled_counter.browser_search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
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
    metrics.labeled_counter.browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content,
    metrics.labeled_counter.browser_search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
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
    metrics.labeled_counter.browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content,
    metrics.labeled_counter.browser_search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
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
    metrics.labeled_counter.browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content,
    metrics.labeled_counter.browser_search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
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
    metrics.labeled_counter.browser_search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content,
    metrics.labeled_counter.browser_search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
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
fenix_combined_searches AS (
  SELECT
    * EXCEPT (
      metrics_search_count,
      browser_search_ad_clicks,
      browser_search_in_content,
      browser_search_with_ads
    ),
    ARRAY_CONCAT(
      add_search_type(metrics_search_count, 'sap'),
      add_search_type(browser_search_in_content, 'in-content'),
      add_search_type(browser_search_ad_clicks, 'ad-click'),
      add_search_type(browser_search_with_ads, 'search-with-ads')
    ) AS searches,
  FROM
    fenix_metrics
),
fenix_flattened_searches AS (
  SELECT
    *,
    CASE
    WHEN
      search.search_type = 'sap'
    THEN
      normalize_fenix_search_key(search.key)[SAFE_OFFSET(0)]
    WHEN
      search.search_type = 'in-content'
    THEN
      SPLIT(search.key, '.')[SAFE_OFFSET(0)]
    WHEN
      search.search_type = 'ad-click'
      OR search.search_type = 'search-with-ads'
    THEN
      search.key
    ELSE
      NULL
    END
    AS engine,
    CASE
    WHEN
      search.search_type = 'sap'
    THEN
      normalize_fenix_search_key(search.key)[SAFE_OFFSET(1)]
    WHEN
      search.search_type = 'in-content'
    THEN
      IF(
        ARRAY_LENGTH(SPLIT(search.key, '.')) < 2,
        NULL,
        ARRAY_TO_STRING(udf.array_slice(SPLIT(search.key, '.'), 1, 3), '.')
      )
    ELSE
      search.search_type
    END
    AS source,
    search.value AS search_count,
    UNIX_DATE(udf.parse_iso8601_date(first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(
      udf.parse_iso8601_date(end_time),
      udf.parse_iso8601_date(first_run_date),
      DAY
    ) AS profile_age_in_days,
  FROM
    fenix_combined_searches
  LEFT JOIN
    fenix_client_locales
  USING
    (client_id)
  CROSS JOIN
    UNNEST(
      -- Add a null search to pings that have no searches
      IF(ARRAY_LENGTH(searches) = 0, add_search_type(null_search(), CAST(NULL AS STRING)), searches)
    ) AS search
),
combined_search_clients AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    normalized_search_key[SAFE_OFFSET(0)] AS engine,
    normalized_search_key[SAFE_OFFSET(1)] AS source,
    'sap' AS search_type,
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
    sample_id,
    normalize_core_experiments(experiments) AS experiments,
  FROM
    core_flattened_searches
  UNION ALL
  SELECT
    submission_date,
    client_id,
    engine,
    source,
    CASE
    WHEN
      STARTS_WITH(source, 'in-content.sap.')
    THEN
      'tagged-sap'
    WHEN
      STARTS_WITH(source, 'in-content.sap-follow-on.')
    THEN
      'tagged-follow-on'
    WHEN
      STARTS_WITH(source, 'in-content.organic')
    THEN
      'organic'
    WHEN
      search_type = 'ad-click'
      OR search_type = 'search-with-ads'
      OR search_type = 'sap'
    THEN
      search_type
    ELSE
      'unknown'
    END
    AS search_type,
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
    sample_id,
    normalize_fenix_experiments(experiments) AS experiments,
  FROM
    fenix_flattened_searches
),
unfiltered_search_clients AS (
  SELECT
    submission_date,
    client_id,
    IF(search_count > 10000, NULL, engine) AS engine,
    IF(search_count > 10000, NULL, source) AS source,
    app_name,
    normalized_app_name,
    SUM(
      IF(search_type != 'sap' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS search_count,
    SUM(
      IF(search_type != 'organic' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS organic,
    SUM(
      IF(search_type != 'tagged-sap' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS tagged_sap,
    SUM(
      IF(
        search_type != 'tagged-follow-on'
        OR engine IS NULL
        OR search_count > 10000,
        0,
        search_count
      )
    ) AS tagged_follow_on,
    SUM(
      IF(search_type != 'ad-click' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS ad_click,
    SUM(
      IF(
        search_type != 'search-with-ads'
        OR engine IS NULL
        OR search_count > 10000,
        0,
        search_count
      )
    ) AS search_with_ads,
    SUM(
      IF(search_type != 'unknown' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS unknown,
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
    udf.mode_last(ARRAY_AGG(sample_id)) AS sample_id,
    udf.map_mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments,
  FROM
    combined_search_clients
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id,
    engine,
    source,
    search_type,
    app_name,
    normalized_app_name
)
SELECT
  *
FROM
  unfiltered_search_clients
