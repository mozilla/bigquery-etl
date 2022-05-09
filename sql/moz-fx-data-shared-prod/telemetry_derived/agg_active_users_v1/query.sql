-- Aggregated users data joined with Country Lookup
SELECT
  submission_date,
  country.name AS country,
  city,
  activity_segment AS segment,
  normalized_app_name AS app_name,
  app_version AS app_version,
  normalized_channel AS channel,
  distribution_id AS distribution,
  normalized_os AS os,
  normalized_os_version AS os_version,
  os_version_major,
  os_version_minor,
  locale AS language,
  attribution_content,
  attribution_source,
  attribution_medium,
  attribution_campaign,
  attribution_experiment,
  attribution_variation,
  COUNT(DISTINCT client_id) AS mau,
  COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau,
  COUNT(DISTINCT IF(days_since_seen = 0, client_id, NULL)) AS dau
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1` metrics
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` country
ON
  metrics.country = country.code
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  country,
  city,
  activity_segment,
  normalized_app_name,
  app_version,
  normalized_channel,
  distribution_id,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor,
  locale,
  attribution_content,
  attribution_source,
  attribution_medium,
  attribution_campaign,
  attribution_experiment,
  attribution_variation
