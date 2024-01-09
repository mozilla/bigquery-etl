--- Query generated via sql_generators.active_users.
WITH todays_metrics AS (
  SELECT
    client_id,
    activity_segments_v1 AS segment,
    'Firefox Desktop' AS app_name,
    app_version AS app_version,
    normalized_channel AS channel,
    IFNULL(country, '??') country,
    city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    first_seen_date,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    days_since_seen,
    os,
    COALESCE(
      `mozfun.norm.windows_version_info`(os, normalized_os_version, windows_build_number),
      normalized_os_version
    ) AS os_version,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    submission_date,
    COALESCE(
      scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
      scalar_parent_browser_engagement_total_uri_count_sum
    ) AS uri_count,
    is_default_browser,
    distribution_id,
    attribution.source AS attribution_source,
    attribution.medium AS attribution_medium,
    attribution.medium IS NOT NULL
    OR attribution.source IS NOT NULL AS attributed,
    ad_clicks_count_all AS ad_click,
    search_count_organic AS organic_search_count,
    search_count_all AS search_count,
    search_with_ads_count_all AS search_with_ads,
    active_hours_sum
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date = @submission_date
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
    ad_click,
    organic_search_count,
    search_count,
    search_with_ads,
    uri_count,
    active_hours_sum,
    first_seen_date
  ),
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS daily_users,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS weekly_users,
  COUNT(DISTINCT client_id) AS monthly_users,
  COUNT(
    DISTINCT IF(days_since_seen = 0 AND active_hours_sum > 0 AND uri_count > 0, client_id, NULL)
  ) AS dau,
  COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
  SUM(ad_click) AS ad_clicks,
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
  segment
