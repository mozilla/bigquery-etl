-- Query generated by bigquery-etl/search/mobile_search_clients_daily.py
--
-- Older versions separate source and engine with an underscore instead of period
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_fenix_search_key(key STRING) AS (
  (
    CASE
      WHEN ARRAY_LENGTH(SPLIT(key, '_')) = 2
        THEN SPLIT(key, '_')
      WHEN ARRAY_LENGTH(SPLIT(key, '.')) = 2
        THEN SPLIT(key, '.')
      ELSE []
    END
  )
);

-- Older versions have source.engine instead of engine.source
-- Assume action is one of actionbar, listitem, suggestion, or quicksearch
-- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/data/core-ping.html#searches
-- Return array of form [source, engine] if key is valid, empty array otherwise
CREATE TEMP FUNCTION normalize_core_search_key(key STRING) AS (
  CASE
    WHEN ARRAY_LENGTH(SPLIT(key, '.')) != 2
      THEN []
    WHEN SPLIT(key, '.')[OFFSET(0)] IN UNNEST(
        ['actionbar', 'listitem', 'suggestion', 'quicksearch']
      )
      THEN ARRAY_REVERSE(SPLIT(key, '.'))
    ELSE SPLIT(key, '.')
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

CREATE TEMP FUNCTION extract_ios_provider(list ARRAY<STRUCT<key STRING, value INT64>>) AS (
  ARRAY(SELECT STRUCT(SPLIT(key, '-')[SAFE_OFFSET(1)] AS key, value AS value) FROM UNNEST(list))
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
  WHERE
    NOT (  -- Filter out newer versions of Firefox iOS in favour of Glean pings
      normalized_os = 'iOS'
      AND normalized_app_name = 'Fennec'
      AND mozfun.norm.truncate_version(metadata.uri.app_version, 'major') >= 28
    )
),
-- baseline for Firefox Preview beta
baseline_org_mozilla_fenix AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fenix.baseline
),
-- metrics for Firefox Preview beta
metrics_org_mozilla_fenix AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Firefox Preview' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'beta' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    org_mozilla_fenix.metrics AS org_mozilla_fenix_metrics
),
-- baseline for Firefox Preview nightly
baseline_org_mozilla_fenix_nightly AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fenix_nightly.baseline
),
-- metrics for Firefox Preview nightly
metrics_org_mozilla_fenix_nightly AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Firefox Preview' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'nightly' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    org_mozilla_fenix_nightly.metrics AS org_mozilla_fenix_nightly_metrics
),
-- baseline for Fenix nightly
baseline_org_mozilla_fennec_aurora AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_fennec_aurora.baseline
),
-- metrics for Fenix nightly
metrics_org_mozilla_fennec_aurora AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'nightly' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    org_mozilla_fennec_aurora.metrics AS org_mozilla_fennec_aurora_metrics
),
-- baseline for Fenix beta
baseline_org_mozilla_firefox_beta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_firefox_beta.baseline
),
-- metrics for Fenix beta
metrics_org_mozilla_firefox_beta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'beta' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    org_mozilla_firefox_beta.metrics AS org_mozilla_firefox_beta_metrics
),
-- baseline for Fenix release
baseline_org_mozilla_firefox AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    client_info.locale
  FROM
    org_mozilla_firefox.baseline
),
-- metrics for Fenix release
metrics_org_mozilla_firefox AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fenix' AS app_name,
    'Fenix' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine_code AS default_search_engine,
    metrics.string.search_default_engine_submission_url AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.metrics_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.events_total_uri_count AS total_uri_count,
  FROM
    org_mozilla_firefox.metrics AS org_mozilla_firefox_metrics
),
-- metrics for Firefox iOS release
metrics_org_mozilla_ios_firefox AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fennec' AS app_name,
    -- Fennec is used to be consistent with core pings
    'Fennec' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.os_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.search_counts AS search_count,
    extract_ios_provider(metrics.labeled_counter.browser_search_ad_clicks) AS search_ad_clicks,
    metrics.labeled_counter.search_in_content AS search_in_content,
    extract_ios_provider(metrics.labeled_counter.browser_search_with_ads) AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    NULL AS total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_ios_firefox.metrics AS org_mozilla_ios_firefox_metrics
  WHERE
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') >= 28
),
-- metrics for Firefox iOS beta
metrics_org_mozilla_ios_firefoxbeta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fennec' AS app_name,
    -- Fennec is used to be consistent with core pings
    'Fennec' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'beta' AS channel,
    normalized_os AS os,
    client_info.os_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.search_counts AS search_count,
    extract_ios_provider(metrics.labeled_counter.browser_search_ad_clicks) AS search_ad_clicks,
    metrics.labeled_counter.search_in_content AS search_in_content,
    extract_ios_provider(metrics.labeled_counter.browser_search_with_ads) AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    NULL AS total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_ios_firefoxbeta.metrics AS org_mozilla_ios_firefoxbeta_metrics
  WHERE
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') >= 28
),
-- metrics for Firefox iOS nightly
metrics_org_mozilla_ios_fennec AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Fennec' AS app_name,
    -- Fennec is used to be consistent with core pings
    'Fennec' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'nightly' AS channel,
    normalized_os AS os,
    client_info.os_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.search_counts AS search_count,
    extract_ios_provider(metrics.labeled_counter.browser_search_ad_clicks) AS search_ad_clicks,
    metrics.labeled_counter.search_in_content AS search_in_content,
    extract_ios_provider(metrics.labeled_counter.browser_search_with_ads) AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    NULL AS total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_ios_fennec.metrics AS org_mozilla_ios_fennec_metrics
  WHERE
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') >= 28
),
-- metrics for Focus Android Glean release
metrics_org_mozilla_focus AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Focus Android Glean' AS app_name,
    'Focus' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.browser_default_search_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_focus.metrics AS org_mozilla_focus_metrics
),
-- metrics for Focus Android Glean beta
metrics_org_mozilla_focus_beta AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Focus Android Glean' AS app_name,
    'Focus' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'beta' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.browser_default_search_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_focus_beta.metrics AS org_mozilla_focus_beta_metrics
),
-- metrics for Focus Android Glean nightly
metrics_org_mozilla_focus_nightly AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Focus Android Glean' AS app_name,
    'Focus' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'nightly' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.browser_default_search_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_focus_nightly.metrics AS org_mozilla_focus_nightly_metrics
),
-- metrics for Klar Android Glean release
metrics_org_mozilla_klar AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Klar Android Glean' AS app_name,
    'Klar' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.browser_default_search_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_klar.metrics AS org_mozilla_klar_metrics
),
-- metrics for Focus iOS Glean release
metrics_org_mozilla_ios_focus AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Focus iOS Glean' AS app_name,
    'Focus' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_ios_focus.metrics AS org_mozilla_ios_focus_metrics
),
-- metrics for Klar iOS Glean release
metrics_org_mozilla_ios_klar AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    normalized_country_code AS country,
    'Klar iOS Glean' AS app_name,
    'Klar' AS normalized_app_name,
    client_info.app_display_version AS app_version,
    'release' AS channel,
    normalized_os AS os,
    client_info.android_sdk_version AS os_version,
    metrics.string.search_default_engine AS default_search_engine,
    CAST(NULL AS STRING) AS default_search_engine_submission_url,
    sample_id,
    metrics.labeled_counter.browser_search_search_count AS search_count,
    metrics.labeled_counter.browser_search_ad_clicks AS search_ad_clicks,
    metrics.labeled_counter.browser_search_in_content AS search_in_content,
    metrics.labeled_counter.browser_search_with_ads AS search_with_ads,
    client_info.first_run_date,
    ping_info.end_time,
    ping_info.experiments,
    metrics.counter.browser_total_uri_count,
    client_info.locale,
  FROM
    org_mozilla_ios_klar.metrics AS org_mozilla_ios_klar_metrics
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
ios_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_ios_firefox
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_ios_firefoxbeta
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_ios_fennec
),
android_focus_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_focus
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_focus_beta
  UNION ALL
  SELECT
    *
  FROM
    metrics_org_mozilla_focus_nightly
),
android_klar_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_klar
),
ios_focus_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_ios_focus
),
ios_klar_metrics AS (
  SELECT
    *
  FROM
    metrics_org_mozilla_ios_klar
),
-- iOS organic counts are incorrect until version 34.0
-- https://github.com/mozilla-mobile/firefox-ios/issues/8412
ios_organic_filtered AS (
  SELECT
    * REPLACE (
      IF(
        mozfun.norm.truncate_version(app_version, 'major') >= 34,
        search_in_content,
        ARRAY(
          SELECT AS STRUCT
            *
          FROM
            UNNEST(search_in_content)
          WHERE
            NOT REGEXP_CONTAINS(key, '\\.organic\\.')
        )
      ) AS search_in_content
    ),
  FROM
    ios_metrics
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
fenix_metrics_with_locale AS (
  SELECT
    fenix_metrics.*,
    locale,
  FROM
    fenix_metrics
  LEFT JOIN
    fenix_client_locales
    USING (client_id)
),
glean_metrics AS (
  SELECT
    *
  FROM
    fenix_metrics_with_locale
  UNION ALL
  SELECT
    *
  FROM
    ios_organic_filtered
  UNION ALL
  SELECT
    *
  FROM
    android_focus_metrics
  UNION ALL
  SELECT
    *
  FROM
    android_klar_metrics
  UNION ALL
  SELECT
    *
  FROM
    ios_focus_metrics
  UNION ALL
  SELECT
    *
  FROM
    ios_klar_metrics
),
glean_combined_searches AS (
  SELECT
    * EXCEPT (search_count, search_ad_clicks, search_in_content, search_with_ads),
    ARRAY_CONCAT(
      add_search_type(search_count, 'sap'),
      add_search_type(search_in_content, 'in-content'),
      add_search_type(search_ad_clicks, 'ad-click'),
      add_search_type(search_with_ads, 'search-with-ads')
    ) AS searches,
  FROM
    glean_metrics
),
glean_flattened_searches AS (
  SELECT
    *,
    CASE
      WHEN search.search_type = 'sap'
        THEN normalize_fenix_search_key(search.key)[SAFE_OFFSET(0)]
      WHEN search.search_type = 'in-content'
        -- key format is engine.in-content.type.code
        THEN SPLIT(search.key, '.')[SAFE_OFFSET(0)]
      WHEN search.search_type = 'ad-click'
        OR search.search_type = 'search-with-ads'
        -- ad-click key format is engine.in-content.type.code for builds starting 2021-03-16
        -- otherwise key is engine
        THEN SPLIT(search.key, '.')[SAFE_OFFSET(0)]
      ELSE NULL
    END AS engine,
    CASE
      WHEN search.search_type = 'sap'
        THEN normalize_fenix_search_key(search.key)[SAFE_OFFSET(1)]
      WHEN search.search_type = 'in-content'
        -- Drop engine from search key to get source
        -- Key should look like `engine.in-content.sap.code` with possible
        -- additional period-separated segments (e.g. .ts for top sites)
        THEN IF(
            STRPOS(search.key, '.') IN (0, LENGTH(search.key)),
            NULL,
            SUBSTR(search.key, STRPOS(search.key, '.') + 1)
          )
      WHEN search.search_type = 'ad-click'
        THEN IF(
            REGEXP_CONTAINS(search.key, '\\.'),
            SUBSTR(search.key, STRPOS(search.key, '.') + 1),
            search.search_type
          )
      WHEN search.search_type = 'search-with-ads'
        THEN IF(
            REGEXP_CONTAINS(search.key, '\\.'),
            SUBSTR(search.key, STRPOS(search.key, '.') + 1),
            search.search_type
          )
      ELSE search.search_type
    END AS source,
    search.value AS search_count,
    UNIX_DATE(udf.parse_iso8601_date(first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(
      udf.parse_iso8601_date(end_time),
      udf.parse_iso8601_date(first_run_date),
      DAY
    ) AS profile_age_in_days,
  FROM
    glean_combined_searches
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
    CAST(NULL AS INT64) AS total_uri_count,
  FROM
    core_flattened_searches
  UNION ALL
  SELECT
    submission_date,
    client_id,
    engine,
    source,
    CASE
      WHEN search_type = 'ad-click'
        THEN IF(STARTS_WITH(source, 'in-content.organic'), 'ad-click-organic', search_type)
      WHEN search_type = 'search-with-ads'
        THEN IF(STARTS_WITH(source, 'in-content.organic'), 'search-with-ads-organic', search_type)
      WHEN STARTS_WITH(source, 'in-content.sap.')
        THEN 'tagged-sap'
      WHEN REGEXP_CONTAINS(source, '^in-content.*-follow-on')
        THEN 'tagged-follow-on'
      WHEN STARTS_WITH(source, 'in-content.organic')
        OR STARTS_WITH(source, 'organic.')  -- for ios
        THEN 'organic'
      WHEN search_type = 'ad-click'
        OR search_type = 'search-with-ads'
        OR search_type = 'sap'
        THEN search_type
      ELSE 'unknown'
    END AS search_type,
    search_count,
    country,
    locale,
    app_name,
    normalized_app_name,
    app_version,
    channel,
    os,
    os_version,
    default_search_engine,
    default_search_engine_submission_url,
    CAST(NULL AS STRING) AS distribution_id,
    profile_creation_date,
    profile_age_in_days,
    sample_id,
    normalize_fenix_experiments(experiments) AS experiments,
    total_uri_count,
  FROM
    glean_flattened_searches
),
unfiltered_search_clients AS (
  SELECT
    submission_date,
    client_id,
    IF(search_count > 10000, NULL, engine) AS engine,
    IF(search_count > 10000, NULL, source) AS source,
    app_name,
    normalized_app_name,
    -- Filter out results with aggregated search count > 10000
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
        search_type != 'ad-click-organic'
        OR engine IS NULL
        OR search_count > 10000,
        0,
        search_count
      )
    ) AS ad_click_organic,
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
      IF(
        search_type != 'search-with-ads-organic'
        OR engine IS NULL
        OR search_count > 10000,
        0,
        search_count
      )
    ) AS search_with_ads_organic,
    SUM(
      IF(search_type != 'unknown' OR engine IS NULL OR search_count > 10000, 0, search_count)
    ) AS unknown,
    udf.mode_last(ARRAY_AGG(country)) AS country,
    udf.mode_last(ARRAY_AGG(locale)) AS locale,
    udf.mode_last(ARRAY_AGG(app_version)) AS app_version,
    channel,
    udf.mode_last(ARRAY_AGG(os)) AS os,
    udf.mode_last(ARRAY_AGG(os_version)) AS os_version,
    udf.mode_last(ARRAY_AGG(default_search_engine)) AS default_search_engine,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_submission_url)
    ) AS default_search_engine_submission_url,
    udf.mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
    udf.mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
    udf.mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
    ANY_VALUE(sample_id) AS sample_id,
    udf.map_mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments,
    SUM(total_uri_count) AS total_uri_count,
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
    normalized_app_name,
    channel
)
SELECT
  *
FROM
  unfiltered_search_clients