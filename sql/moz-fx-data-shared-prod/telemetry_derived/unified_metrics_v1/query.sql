WITH unioned_source AS (
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
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
    'Fenix' AS normalized_app_name
  FROM
    fenix.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
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
    'Firefox iOS' AS normalized_app_name
  FROM
    firefox_ios.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
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
    'Focus iOS' AS normalized_app_name
  FROM
    focus_ios.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    udf_js.sample_id(client_id) AS sample_id,
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
    'Focus Android' AS normalized_app_name
  FROM
    telemetry.core_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND app_name = 'Focus'
    AND os = 'Android'
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
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
    'Focus Android Glean' AS normalized_app_name
  FROM
    focus_android.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
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
    'Klar iOS' AS normalized_app_name
  FROM
    klar_ios.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
),
unioned AS (
  SELECT
    * REPLACE (
      -- Per bug 1757216 we need to exclude BrowserStack clients from KPIs,
      -- so we mark them with a separate app name here. We expect BrowserStack
      -- clients only on release channel of Fenix, so the only variant this is
      -- expected to produce is 'Fenix BrowserStack'
      IF(
        isp = 'BrowserStack',
        CONCAT(normalized_app_name, ' BrowserStack'),
        normalized_app_name
      ) AS normalized_app_name
    )
  FROM
    unioned_source
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
    search_derived.mobile_search_clients_daily_v1
  WHERE
    submission_date = @submission_date
),
search_metrics AS (
  SELECT
    unioned.client_id,
    unioned.submission_date,
        -- the table is more than one row per client (one row per engine, looks like), so we have to aggregate.
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads,
  FROM
    unioned
  LEFT JOIN
    search_clients s
    ON unioned.client_id = s.client_id
    AND unioned.submission_date = s.submission_date
  GROUP BY
    client_id,
    submission_date
),
mobile_with_searches AS (
  SELECT
    unioned.client_id,
    unioned.sample_id,
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
    unioned.normalized_app_name,
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
    unioned.durations,
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
    search.ad_click,
    search.organic_search_count,
    search.search_count,
    search.search_with_ads,
    NULL AS active_hours_sum
  FROM
    unioned
  LEFT JOIN
    search_metrics search
    ON search.client_id = unioned.client_id
    AND search.submission_date = unioned.submission_date
),
desktop AS (
  SELECT
    client_id,
    sample_id,
    activity_segments_v1 AS activity_segment,
    'Firefox Desktop' AS normalized_app_name,
    app_version AS app_version,
    normalized_channel,
    IFNULL(country, '??') country,
    city,
    days_visited_1_uri_bits AS days_seen_bits,
    days_created_profile_bits,
    days_since_first_seen,
    CAST(NULL AS string) AS device_model,
    isp_name AS isp,
    submission_date = first_seen_date AS is_new_profile,
    locale,
    first_seen_date,
    days_since_seen,
    os AS normalized_os,
    normalized_os_version,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
      0
    ) AS os_version_patch,
    subsession_hours_sum AS durations,
    submission_date,
    COALESCE(
      scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
      scalar_parent_browser_engagement_total_uri_count_sum
    ) AS uri_count,
    is_default_browser,
    distribution_id,
    attribution.content AS attribution_content,
    attribution.source AS attribution_source,
    attribution.medium AS attribution_medium,
    attribution.campaign AS attribution_campaign,
    attribution.experiment AS attribution_experiment,
    attribution.variation AS attribution_variation,
    ad_clicks_count_all AS ad_clicks,
    search_count_organic AS organic_search_count,
    search_count_all AS search_count,
    search_with_ads_count_all AS search_with_ads,
    active_hours_sum
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date = @submission_date
)
SELECT
  *
FROM
  mobile_with_searches
UNION ALL
SELECT
  *
FROM
  desktop
