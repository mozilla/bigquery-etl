-- Query for telemetry_derived.active_users_aggregates_attribution_v1
SELECT
  attribution_medium IS NOT NULL
  OR attribution_source IS NOT NULL AS attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  city,
  country,
  EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
  is_default_browser,
  normalized_app_name AS app_name,
  submission_date,
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
  COUNT(DISTINCT client_id) AS mau,
  COUNT(DISTINCT IF(is_new_profile IS TRUE, client_id, NULL)) AS new_profiles,
  SUM(ad_click) AS ad_clicks,
  SUM(organic_search_count) AS organic_search_count,
  SUM(search_count) AS search_count,
  SUM(search_with_ads) AS search_with_ads,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  attributed,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  city,
  country,
  first_seen_year,
  is_default_browser,
  app_name,
  submission_date
