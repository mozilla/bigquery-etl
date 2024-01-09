--- Query generated via sql_generators.active_users.
WITH baseline AS (
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    days_since_seen,
    days_seen_bits,
    days_created_profile_bits,
    durations,
    os AS normalized_os,
    osversion AS normalized_os_version,
    locale,
    city,
    country,
    metadata_app_version AS app_display_version,
    device AS device_model,
    first_seen_date,
    submission_date = first_seen_date AS is_new_profile,
    NULL AS uri_count,
    default_browser AS is_default_browser,
    distribution_id,
    CAST(NULL AS string) AS isp,
    'Focus Android' AS app_name
  FROM
    `{{ project_id }}.telemetry.core_clients_last_seen`
  WHERE
    submission_date = @submission_date
    AND app_name = 'Focus'
    AND os = 'Android'
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    days_since_seen,
    days_seen_bits,
    days_created_profile_bits,
    durations,
    normalized_os,
    normalized_os_version,
    locale,
    city,
    country,
    app_display_version,
    device_model,
    first_seen_date,
    submission_date = first_seen_date AS is_new_profile,
    uri_count,
    is_default_browser,
    CAST(NULL AS string) AS distribution_id,
    isp,
    'Focus Android Glean' AS app_name
  FROM
    `{{ project_id }}.{{ app_name }}.clients_last_seen_joined`
  WHERE
    submission_date = @submission_date
),
unioned AS (
  SELECT
    * REPLACE (IF(isp = 'BrowserStack', CONCAT(app_name, ' BrowserStack'), app_name) AS app_name)
  FROM
    baseline
),
search_clients AS (
  SELECT
    client_id,
    submission_date,
    ad_click,
    organic,
    search_count,
    search_with_ads
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
  WHERE
    submission_date = @submission_date
),
search_metrics AS (
  SELECT
    unioned.client_id,
    unioned.submission_date,
    SUM(ad_click) AS ad_clicks,
    SUM(organic) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads
  FROM
    unioned
  LEFT JOIN
    search_clients s
  ON
    unioned.client_id = s.client_id
    AND unioned.submission_date = s.submission_date
  GROUP BY
    client_id,
    submission_date
),
unioned_with_searches AS (
  SELECT
    unioned.client_id,
    CASE
      WHEN BIT_COUNT(days_seen_bits)
        BETWEEN 1
        AND 6
        THEN 'infrequent_user'
      WHEN BIT_COUNT(days_seen_bits)
        BETWEEN 7
        AND 13
        THEN 'casual_user'
      WHEN BIT_COUNT(days_seen_bits)
        BETWEEN 14
        AND 20
        THEN 'regular_user'
      WHEN BIT_COUNT(days_seen_bits) >= 21
        THEN 'core_user'
      ELSE 'other'
    END AS activity_segment,
    unioned.app_name,
    unioned.app_display_version AS app_version,
    unioned.normalized_channel,
    IFNULL(country, '??') country,
    unioned.city,
    unioned.days_seen_bits,
    unioned.days_created_profile_bits,
    DATE_DIFF(unioned.submission_date, unioned.first_seen_date, DAY) AS days_since_first_seen,
    unioned.device_model,
    unioned.isp,
    unioned.is_new_profile,
    unioned.locale,
    unioned.first_seen_date,
    unioned.days_since_seen,
    unioned.normalized_os,
    unioned.normalized_os_version,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
      0
    ) AS os_version_patch,
    unioned.durations AS durations,
    unioned.submission_date,
    unioned.uri_count,
    unioned.is_default_browser,
    unioned.distribution_id,
    CAST(NULL AS string) AS attribution_content,
    CAST(NULL AS string) AS attribution_source,
    CAST(NULL AS string) AS attribution_medium,
    CAST(NULL AS string) AS attribution_campaign,
    CAST(NULL AS string) AS attribution_experiment,
    CAST(NULL AS string) AS attribution_variation,
    search.ad_clicks,
    search.organic_search_count,
    search.search_count,
    search.search_with_ads,
    CAST(NULL AS FLOAT64) AS active_hours_sum
  FROM
    unioned
  LEFT JOIN
    search_metrics search
  ON
    search.client_id = unioned.client_id
    AND search.submission_date = unioned.submission_date
),
todays_metrics AS (
  SELECT
    activity_segment AS segment,
    app_version,
    attribution_medium,
    attribution_source,
    attribution_medium IS NOT NULL
    OR attribution_source IS NOT NULL AS attributed,
    city,
    country,
    distribution_id,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    is_default_browser,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    app_name AS app_name,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    os_version_major,
    os_version_minor,
    durations,
    submission_date,
    days_since_seen,
    client_id,
    first_seen_date,
    ad_clicks,
    organic_search_count,
    search_count,
    search_with_ads,
    uri_count,
    active_hours_sum,
    CAST(NULL AS STRING) AS adjust_network,
    CAST(NULL AS STRING) AS install_source
  FROM
    unioned_with_searches
),
todays_metrics_enriched AS (
  SELECT
    todays_metrics.* EXCEPT (locale),
    CASE
      WHEN locale IS NOT NULL
        AND languages.language_name IS NULL
        THEN 'Other'
      ELSE languages.language_name
    END AS language_name,
  FROM
    todays_metrics
  LEFT JOIN
    `mozdata.static.csa_gblmkt_languages` AS languages
  ON
    todays_metrics.locale = languages.code
)
SELECT
  todays_metrics_enriched.* EXCEPT (
    client_id,
    days_since_seen,
    ad_clicks,
    organic_search_count,
    search_count,
    search_with_ads,
    uri_count,
    active_hours_sum,
    first_seen_date,
    durations
  ),
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS daily_users,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS weekly_users,
  COUNT(DISTINCT client_id) AS monthly_users,
  COUNT(DISTINCT IF(days_since_seen = 0 AND durations > 0, client_id, NULL)) AS dau,
  COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
  SUM(ad_clicks) AS ad_clicks,
  SUM(organic_search_count) AS organic_search_count,
  SUM(search_count) AS search_count,
  SUM(search_with_ads) AS search_with_ads,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours,
FROM
  todays_metrics_enriched
GROUP BY
  app_version,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  country,
  distribution_id,
  first_seen_year,
  is_default_browser,
  language_name,
  app_name,
  channel,
  os,
  os_version,
  os_version_major,
  os_version_minor,
  submission_date,
  segment,
  adjust_network,
  install_source
