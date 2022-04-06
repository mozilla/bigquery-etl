WITH unioned AS (
  SELECT
    *,
    'Fenix' AS normalized_app_name
  FROM
    fenix.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    *,
    'Firefox iOS' AS normalized_app_name
  FROM
    firefox_ios.clients_last_seen_joined
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    *,
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
    'Focus Android' AS normalized_app_name
  FROM
    telemetry.core_clients_last_seen
  WHERE
    days_since_seen = 0
    AND app_name = 'Focus'
    AND os = 'Android'
    AND submission_date = @submission_date
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
    unioned.app_display_version,
    unioned.normalized_channel,
    unioned.country,
    unioned.days_seen_bits,
    DATE_DIFF(unioned.submission_date, unioned.first_seen_date, DAY) AS days_since_first_seen,
    unioned.device_model,
    unioned.is_new_profile,
    unioned.locale,
    unioned.first_seen_date,
    unioned.normalized_os,
    unioned.normalized_os_version,
    unioned.durations,
    unioned.submission_date,
    unioned.uri_count,
    unioned.is_default_browser,
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
    app_display_version,
    normalized_channel,
    country,
    days_visited_1_uri_bits AS days_seen_bits,
    days_since_first_seen,
    CAST(NULL AS string) AS device_model,
    submission_date = first_seen_date AS is_new_profile,
    locale,
    first_seen_date,
    os AS normalized_os,
    normalized_os_version,
    subsession_hours_sum AS durations,
    submission_date,
    COALESCE(
      scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
      scalar_parent_browser_engagement_total_uri_count_sum
    ) AS uri_count,
    is_default_browser,
    ad_clicks_count_all AS ad_clicks,
    search_count_organic AS organic_search_count,
    search_count_all AS search_count,
    search_with_ads_count_all AS search_with_ads,
    active_hours_sum
  FROM
    telemetry.clients_last_seen
  WHERE
    days_since_seen = 0
    AND submission_date = @submission_date
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
