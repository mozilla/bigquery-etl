-- Query for telemetry_derived.active_users_aggregates_attribution_v1
WITH baseline AS (
  SELECT
    unified_metrics.client_id,
    attribution_medium IS NOT NULL
    OR attribution_source IS NOT NULL AS attributed,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_variation,
    att.adjust_network,
    att.install_source,
    city,
    country,
    unified_metrics.first_seen_date,
    EXTRACT(YEAR FROM unified_metrics.first_seen_date) AS first_seen_year,
    is_default_browser,
    normalized_app_name,
    unified_metrics.submission_date,
    days_since_seen,
    ad_click,
    organic_search_count,
    search_count,
    search_with_ads,
    uri_count,
    active_hours_sum
  FROM
    `moz-fx-data-shared-prod.telemetry.unified_metrics` AS unified_metrics
  LEFT JOIN
    fenix.firefox_android_clients AS att
    ON unified_metrics.client_id = att.client_id
  WHERE
    unified_metrics.submission_date = @submission_date
)
SELECT
  attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  adjust_network,
  install_source,
  city,
  country,
  first_seen_year,
  is_default_browser,
  normalized_app_name AS app_name,
  submission_date,
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
  COUNT(DISTINCT client_id) AS mau,
  COUNT(
    DISTINCT IF(
      days_since_seen = 0
      AND normalized_app_name = 'Firefox Desktop'
      AND active_hours_sum > 0
      AND uri_count > 0,
      client_id,
      NULL
    )
  ) AS qdau_desktop,
  COUNT(DISTINCT IF(submission_date = first_seen_date, client_id, NULL)) AS new_profiles,
  SUM(ad_click) AS ad_clicks,
  SUM(organic_search_count) AS organic_search_count,
  SUM(search_count) AS search_count,
  SUM(search_with_ads) AS search_with_ads,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours
FROM
  baseline
GROUP BY
  attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  adjust_network,
  install_source,
  city,
  country,
  first_seen_year,
  is_default_browser,
  app_name,
  submission_date
