with staging AS (
  SELECT
    PARSE_DATE('%Y%m%d', a.event_date) AS date,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS language,
    geo.country AS country,
    device.web_info.browser_version AS browser_version,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS content,
    --? AS blog, 
    --? AS subblog,
    --? AS sessions,
    --? AS downloads,
    --? AS social_share
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_*` 
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY 
  date,
  device_category,
  operating_system,
  browser,
  language,
  country,
  browser_version,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog
)

SELECT 
stg.date,
stg.device_category,
stg.operating_system,
stg.browser,
stg.language,
stg.country, 
standardized_country_list.standardized_country_name,
stg.browser_version,
stg.source,
stg.medium,
stg.campaign, 
stg.content, 
stg.blog,
stg.subblog,
stg.sessions,
stg.downloads,
stg.social_share,
stg.newsletter_subscription
FROM staging stg 
LEFT JOIN 
`moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
ON stg.country = standardized_country_list.raw_country
--WHERE -- Note: need to add a filter to only the 'blog.mozilla.org' domain