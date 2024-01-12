WITH site_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS language,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    COUNTIF(event_name = 'session_start') AS sessions,
    COUNTIF(
      event_name = 'session_start'
      AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser)
    ) AS non_fx_sessions,
    COUNTIF(event_name = 'product_download') AS downloads,
    COUNTIF(
      event_name = 'product_download'
      AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser)
    ) AS non_fx_downloads
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    event_date = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    event_date,
    device.category,
    device.operating_system,
    device.web_info.browser,
    device.language,
    geo.country,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
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
  s.ad_content,
  s.sessions,
  s.non_fx_sessions,
  s.downloads,
  s.non_fx_downloads
FROM
  site_data AS s
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS std_cntry_nms
  ON s.country = std_cntry_nms.raw_country
