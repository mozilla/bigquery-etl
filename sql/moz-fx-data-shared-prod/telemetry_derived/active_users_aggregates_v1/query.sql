-- Aggregated clients data including active users, new profiles and search metrics
WITH aggregated AS (
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
    normalized_app_name AS app_name,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
    COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
    COUNT(DISTINCT client_id) AS mau,
    COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
    SUM(ad_click) AS ad_clicks,
    SUM(organic_search_count) AS organic_search_count,
    SUM(search_count) AS search_count,
    SUM(search_with_ads) AS search_with_ads,
    SUM(uri_count) AS uri_count,
    SUM(active_hours_sum) AS active_hours
  FROM
    `moz-fx-data-shared-prod.telemetry.unified_metrics`
  WHERE
    submission_date = @submission_date
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
    locale,
    app_name,
    channel,
    os,
    os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    segment
)
SELECT
  aggregated.* EXCEPT (locale),
  CASE
  WHEN
    locale IS NOT NULL
    AND languages.language_name IS NULL
  THEN
    'Other'
  ELSE
    languages.language_name
  END
  AS language_name
FROM
  aggregated
LEFT JOIN
  `mozdata.static.csa_gblmkt_languages` AS languages
ON
  aggregated.locale = languages.code
