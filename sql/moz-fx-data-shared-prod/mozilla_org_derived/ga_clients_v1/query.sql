-- Important Note: This table is meant to be forwards-compatible
-- with the GA4 schema: https://support.google.com/analytics/answer/7029846?hl=en#zippy=%2Citems%2Cecommerce%2Cstream-and-platform%2Ctraffic-source%2Ccollected-traffic-source
WITH historical_clients AS (
  SELECT
    *
  FROM
    mozdata.analysis.ga_clients_v1
),
new_sessions AS (
  SELECT
    mozdata.analysis.ga_nullify_string(clientId) AS ga_client_id,
    MIN(PARSE_DATE('%Y%m%d', date)) AS first_seen_date,
    MAX(PARSE_DATE('%Y%m%d', date)) AS last_seen_date,
    STRUCT(
      /* Geos */
      MIN_BY(geoNetwork.country, visitNumber) AS country,
      MIN_BY(geoNetwork.region, visitNumber) AS region,
      MIN_BY(geoNetwork.city, visitNumber) AS city,
      /* Attribution */
      MIN_BY(CAST(trafficSource.adwordsClickInfo.campaignId AS STRING), visitNumber) AS campaign_id,
      MIN_BY(trafficSource.campaign, visitNumber) AS campaign,
      MIN_BY(trafficSource.source, visitNumber) AS source,
      MIN_BY(trafficSource.medium, visitNumber) AS medium,
      MIN_BY(trafficSource.keyword, visitNumber) AS term,
      MIN_BY(trafficSource.adContent, visitNumber) AS content,
      /* Device */
      MIN_BY(device.deviceCategory, visitNumber) AS device_category,
      MIN_BY(device.mobileDeviceModel, visitNumber) AS mobile_device_model,
      MIN_BY(device.mobileDeviceInfo, visitNumber) AS mobile_device_string,
      MIN_BY(device.operatingSystem, visitNumber) AS os,
      MIN_BY(device.operatingSystemVersion, visitNumber) AS os_version,
      MIN_BY(device.language, visitNumber) AS language,
      MIN_BY(device.browser, visitNumber) AS browser,
      MIN_BY(device.browserVersion, visitNumber) AS browser_version
    ) AS first_reported,
    STRUCT(
      /* Geos */
      MAX_BY(geoNetwork.country, visitNumber) AS country,
      MAX_BY(geoNetwork.region, visitNumber) AS region,
      MAX_BY(geoNetwork.city, visitNumber) AS city,
      /* Attribution */
      MAX_BY(CAST(trafficSource.adwordsClickInfo.campaignId AS STRING), visitNumber) AS campaign_id,
      MAX_BY(trafficSource.campaign, visitNumber) AS campaign,
      MAX_BY(trafficSource.source, visitNumber) AS source,
      MAX_BY(trafficSource.medium, visitNumber) AS medium,
      MAX_BY(trafficSource.keyword, visitNumber) AS term,
      MAX_BY(trafficSource.adContent, visitNumber) AS content,
      /* Device */
      MAX_BY(device.deviceCategory, visitNumber) AS device_category,
      MAX_BY(device.mobileDeviceModel, visitNumber) AS mobile_device_model,
      MAX_BY(device.mobileDeviceInfo, visitNumber) AS mobile_device_string,
      MAX_BY(device.operatingSystem, visitNumber) AS os,
      MAX_BY(device.operatingSystemVersion, visitNumber) AS os_version,
      MAX_BY(device.language, visitNumber) AS language,
      MAX_BY(device.browser, visitNumber) AS browser,
      MAX_BY(device.browserVersion, visitNumber) AS browser_version
    ) AS last_reported,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  WHERE
    -- Re-process yesterday, to account for late-arriving data
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@session_date, INTERVAL 1 DAY))
    AND FORMAT_DATE('%Y%m%d', @session_date)
  GROUP BY
    ga_client_id
  HAVING
    ga_client_id IS NOT NULL
)
SELECT
  ga_client_id,
  -- Least and greatest return NULL if any input is NULL, so we coalesce each value first
  LEAST(
    COALESCE(_previous.first_seen_date, _current.first_seen_date),
    COALESCE(_current.first_seen_date, _previous.first_seen_date)
  ) AS first_seen_date,
  GREATEST(
    COALESCE(_previous.last_seen_date, _current.last_seen_date),
    COALESCE(_current.last_seen_date, _previous.last_seen_date)
  ) AS last_seen_date,
  -- We take the current data for first_reported only if we don't already have first_reported data, or if we're backfilling
  IF(
    _previous.ga_client_id IS NULL
    OR _current.first_seen_date < _previous.first_seen_date,
    _current.first_reported,
    _previous.first_reported
  ) AS first_reported,
  -- We take the current data for last_reported only if we have it, and this current data is indeed newer than our previous data
  IF(
    _current.ga_client_id IS NULL
    OR _previous.last_seen_date > _current.last_seen_date,
    _previous.last_reported,
    _current.last_reported
  ) AS last_reported,
FROM
  historical_clients AS _previous
FULL OUTER JOIN
  new_sessions AS _current
USING
  (ga_client_id)
