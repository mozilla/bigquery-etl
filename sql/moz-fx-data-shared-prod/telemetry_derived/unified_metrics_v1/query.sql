WITH unioned AS (
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    sample_id,
    days_since_seen,
    days_since_created_profile,
    days_since_seen_session_start,
    days_since_seen_session_end,
    days_seen_bits,
    days_created_profile_bits,
    first_run_date,
    durations,
    days_seen_session_start_bits,
    days_seen_session_end_bits,
    normalized_os,
    normalized_os_version,
    android_sdk_version,
    locale,
    city,
    country,
    app_build,
    app_channel,
    app_display_version,
    architecture,
    device_manufacturer,
    device_model,
    telemetry_sdk_build,
    first_seen_date,
    is_new_profile,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    uri_count,
    is_default_browser,
    isp,
    CAST(NULL AS string) AS distribution_id,
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
    days_since_created_profile,
    days_since_seen_session_start,
    days_since_seen_session_end,
    days_seen_bits,
    days_created_profile_bits,
    first_run_date,
    durations,
    days_seen_session_start_bits,
    days_seen_session_end_bits,
    normalized_os,
    normalized_os_version,
    android_sdk_version,
    locale,
    city,
    country,
    app_build,
    app_channel,
    app_display_version,
    architecture,
    device_manufacturer,
    device_model,
    telemetry_sdk_build,
    first_seen_date,
    is_new_profile,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    uri_count,
    is_default_browser,
    isp,
    CAST(NULL AS string) AS distribution_id,
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
    days_since_created_profile,
    days_since_seen_session_start,
    days_since_seen_session_end,
    days_seen_bits,
    days_created_profile_bits,
    first_run_date,
    durations,
    days_seen_session_start_bits,
    days_seen_session_end_bits,
    normalized_os,
    normalized_os_version,
    android_sdk_version,
    locale,
    city,
    country,
    app_build,
    app_channel,
    app_display_version,
    architecture,
    device_manufacturer,
    device_model,
    telemetry_sdk_build,
    first_seen_date,
    is_new_profile,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    uri_count,
    is_default_browser,
    isp,
    CAST(NULL AS string) AS distribution_id,
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
    days_since_created_profile,
    NULL AS days_since_seen_session_start,
    NULL AS days_since_seen_session_end,
    days_seen_bits,
    days_created_profile_bits,
    first_seen_date AS first_run_date,
    durations,
    NULL AS days_seen_session_start_bits,
    NULL AS days_seen_session_end_bits,
    os AS normalized_os,
    osversion AS normalized_os_version,
    NULL AS android_sdk_version,
    locale,
    city,
    country,
    app_build_id AS app_build,
    NULL AS app_channel,
    metadata_app_version AS app_display_version,
    NULL AS architecture,
    NULL AS device_manufacturer,
    device AS device_model,
    NULL AS telemetry_sdk_build,
    first_seen_date,
    submission_date = first_seen_date AS is_new_profile,
    NULL AS n_metrics_ping,
    NULL AS days_sent_metrics_ping_bits,
    NULL AS uri_count,
    default_browser AS is_default_browser,
    NULL AS isp,
    distribution_id AS distribution_id,
    'Focus Android' AS normalized_app_name
  FROM
    telemetry.core_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND app_name = 'Focus'
    AND os = 'Android'
),
search_clients AS (
  SELECT
    *
  FROM
    search_derived.mobile_search_clients_daily_v1
  WHERE
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
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
    search_clients m
  ON
    unioned.client_id = m.client_id
    AND DATE_ADD(unioned.submission_date, INTERVAL 1 DAY) = m.submission_date
  GROUP BY
    1,
    2
),
mobile_with_searches AS (
  SELECT
    unioned.client_id,
    unioned.sample_id,
    CASE
    WHEN
      BIT_COUNT(days_seen_bits)
      BETWEEN 1
      AND 6
    THEN
      'infrequent_user'
    WHEN
      BIT_COUNT(days_seen_bits)
      BETWEEN 7
      AND 13
    THEN
      'casual_user'
    WHEN
      BIT_COUNT(days_seen_bits)
      BETWEEN 14
      AND 20
    THEN
      'regular_user'
    WHEN
      BIT_COUNT(days_seen_bits) >= 21
    THEN
      'core_user'
    ELSE
      'other'
    END
    AS activity_segment,
    unioned.normalized_app_name,
    unioned.app_display_version AS app_version,
    unioned.normalized_channel,
    unioned.country,
    unioned.city,
    unioned.days_seen_bits,
    DATE_DIFF(unioned.submission_date, unioned.first_seen_date, DAY) AS days_since_first_seen,
    unioned.device_model,
    unioned.is_new_profile,
    unioned.locale,
    unioned.first_seen_date,
    unioned.days_since_seen,
    unioned.normalized_os,
    unioned.normalized_os_version,
    COALESCE(
      CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(
      CAST(NULLIF(SPLIT(unioned.normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
      0
    ) AS os_version_patch,
    unioned.durations,
    unioned.submission_date,
    unioned.uri_count,
    unioned.is_default_browser,
    unioned.distribution_id AS distribution_id,
    CAST(NULL AS string) AS attribution_content,
    CAST(NULL AS string) AS attribution_source,
    CAST(NULL AS string) AS attribution_medium,
    CAST(NULL AS string) AS attribution_campaign,
    CAST(NULL AS string) AS attribution_experiment,
    CAST(NULL AS string) AS attribution_variation,
    search.* EXCEPT (submission_date, client_id),
    NULL AS active_hours_sum
  FROM
    unioned
  LEFT JOIN
    search_metrics search
  ON
    search.client_id = unioned.client_id
    AND search.submission_date = DATE_ADD(unioned.submission_date, INTERVAL 1 DAY)
),
desktop AS (
  SELECT
    client_id,
    sample_id,
    activity_segments_v1 AS activity_segment,
    'Firefox Desktop' AS normalized_app_name,
    app_version AS app_version,
    normalized_channel,
    country,
    city,
    days_visited_1_uri_bits AS days_seen_bits,
    days_since_first_seen,
    CAST(NULL AS string) AS device_model,
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
    distribution_id AS distribution_id,
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
