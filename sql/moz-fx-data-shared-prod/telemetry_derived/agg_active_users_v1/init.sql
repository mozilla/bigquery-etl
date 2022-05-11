CREATE OR REPLACE TABLE
  `mozilla-public-data`.telemetry_derived.unified_metrics_v1
PARTITION BY
  DATE(submission_date)
AS
SELECT
  activity_segment AS segment,
  app_version AS app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  city,
  country,
  device_model,
  distribution_id,
  is_default_browser,
  locale,
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
  COUNT(DISTINCT IF(is_new_profile IS TRUE, client_id, NULL)) AS new_profiles,
  SUM(ad_click) AS ad_clicks,
  SUM(organic_search_count) AS organic_search_count,
  SUM(search_count) AS search_count,
  SUM(search_with_ads) AS search_with_ads
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  city,
  country,
  device_model,
  distribution_id,
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
