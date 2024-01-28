SELECT
  PARSE_DATE('%Y%m%d', event_date) AS date,
  a.user_pseudo_id || '-' || CAST(e.value.int_value as string) AS visit_identifier,
  device.category AS device_category,
  device.operating_system AS operating_system,
  device.language AS `language`,
  geo.country AS country,
  collected_traffic_source.manual_source AS source,
  collected_traffic_source.manual_medium AS medium,
  collected_traffic_source.manual_campaign_name AS campaign,
  collected_traffic_source.manual_content AS ad_content,
  device.web_info.browser AS browser,
  --note: the 2 columns are the same because in GA4, there is no logic saying you can only count 1 download per session, unlike GA3
  COUNTIF(event_name = 'product_download') AS download_events,
  COUNTIF(event_name = 'product_download') AS downloads,
  COUNTIF(
    event_name = 'product_download'
    AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser)
  ) AS non_fx_downloads,
FROM
  `moz-fx-data-marketing-prod.analytics_313696158.events_*` AS a
JOIN
  UNNEST(event_params) AS e
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  AND e.key = 'ga_session_id'
  AND a.event_name = 'product_download'
GROUP BY
  date,
  visit_identifier,
  device_category,
  operating_system,
  `LANGUAGE`,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser
