-- Query for ga_derived.www_site_metrics_summary_v2
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH site_data AS (
  SELECT
    event_date as date,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS language,
    geo.country AS country,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    sum(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
    sum(CASE WHEN event_name = 'session_start' AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser) THEN 1 ELSE 0 END) AS non_fx_sessions,
    sum(CASE WHEN event_name = 'product_download' THEN 1 ELSE 0 END) AS downloads,
    sum(CASE WHEN event_name = 'product_download' AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser) THEN 1 ELSE 0 END) AS non_fx_downloads
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  where
    event_date = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    event_date,
    device.category,
    device.operating_system,
    device.web_info.browser,
    device.language,
    geo.country,
    traffic_source.source,
    traffic_source.medium,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_content
)

SELECT
  s.date,
  s.device_category,
  s.operating_system,
  s.browser,
  s.language,
  s.country,
  std_cntry_nms.standardized_country AS standardized_country_name,
  s.source,
  s.medium,
  s.campaign,
  s.ad_content
  s.sessions,
  s.non_fx_sessions,
  s.downloads,
  s.non_fx_downloads
FROM
  site_data s
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` std_cntry_nms
ON s.country = std_cntry_nms.raw_country