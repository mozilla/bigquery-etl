-- Query for telemetry_derived.active_users_aggregates_device_v1
SELECT
  app_version,
  attribution_medium IS NOT NULL
  OR attribution_source IS NOT NULL AS attributed,
  attribution_medium,
  attribution_source,
  country,
  device_model,
  EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
  is_default_browser,
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
  `moz-fx-data-shared-prod.telemetry.unified_metrics`
WHERE
  submission_date = @submission_date
GROUP BY
  app_version,
  attributed,
  attribution_medium,
  attribution_source,
  country,
  device_model,
  first_seen_year,
  is_default_browser,
  app_name,
  channel,
  os,
  os_version,
  os_version_major,
  os_version_minor,
  submission_date
