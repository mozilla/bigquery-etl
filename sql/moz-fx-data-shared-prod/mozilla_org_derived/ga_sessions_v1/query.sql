-- First note: This table is meant to be forwards-compatible
--   with the GA4 schema: https://support.google.com/analytics/answer/7029846
--   But that's harder, since some of the data is contained within events there (e.g. session_start is an event_param, with the value as the session_id)
--   See https://www.ga4bigquery.com/sessions-dimensions-metrics-ga4/
-- Second note: We do not store user_ids, only client_ids
--   After migration client_ids will be called pseudo_user_ids, see
--   https://louder.com.au/2022/06/27/client-id-in-ga4-what-is-it-and-how-to-get-it-in-your-report/
-- Third note: The only non-forwards-compatible field is mobileDeviceInfo
--   in GA4, that will be split into its components (model, manufacturer, etc.)
--   I think we can simply handle this in the view using some UDFs
-- Fourth note: Data is updated up to three days after the event happens, see
--   https://support.google.com/analytics/answer/7029846?#tables
CREATE TEMP FUNCTION normalize_install_target(target STRING)
RETURNS STRING AS (
  -- See https://sql.telemetry.mozilla.org/queries/95883/source
  CASE
    WHEN target LIKE "Firefox for Desktop%"
      THEN "desktop_release"
    WHEN target LIKE "Firefox ESR%"
      THEN "desktop_esr"
    WHEN target LIKE "Firefox Developer Edition%"
      THEN "desktop_developer_edition"
    WHEN target LIKE "Firefox Beta%"
      THEN "desktop_beta"
    WHEN target LIKE "Firefox Nightly Edition%"
      THEN "desktop_nightly"
    WHEN target LIKE "Firefox for Android%"
      THEN "android_release"
    WHEN target LIKE "Firefox Beta Android%"
      THEN "android_beta"
    WHEN target LIKE "Firefox for iOS%"
      THEN "ios_release"
    ELSE NULL
  END
);

WITH daily_sessions AS (
  SELECT
    mozfun.ga.nullify_string(clientId) AS ga_client_id,
    -- visitId (or sessionId in GA4) is guaranteed unique only among one client, look at visitId here https://support.google.com/analytics/answer/3437719?hl=en
    CONCAT(mozfun.ga.nullify_string(clientId), CAST(visitId AS STRING)) AS ga_session_id,
    MIN(PARSE_DATE('%Y%m%d', date)) AS session_date,
    MIN(visitNumber) = 1 AS is_first_session,
    MIN(visitNumber) AS session_number,
    ARRAY_CONCAT_AGG(hits) AS hits,
    SUM(totals.timeOnSite) AS time_on_site,
    SUM(totals.pageviews) AS pageviews,
    /* Geos */
    MIN_BY(geoNetwork.country, visitStartTime) AS country,
    MIN_BY(geoNetwork.region, visitStartTime) AS region,
    MIN_BY(geoNetwork.city, visitStartTime) AS city,
    /* Attribution */
    MIN_BY(
      CAST(trafficSource.adwordsClickInfo.campaignId AS STRING),
      visitStartTime
    ) AS campaign_id,
    MIN_BY(trafficSource.campaign, visitStartTime) AS campaign,
    MIN_BY(trafficSource.source, visitStartTime) AS source,
    MIN_BY(trafficSource.medium, visitStartTime) AS medium,
    MIN_BY(trafficSource.keyword, visitStartTime) AS term,
    MIN_BY(trafficSource.adContent, visitStartTime) AS content,
    ARRAY_AGG(mozfun.ga.nullify_string(trafficSource.adwordsClickInfo.gclId) IGNORE NULLS)[
      0
    ] AS gclid,
    /* Device */
    MIN_BY(device.deviceCategory, visitStartTime) AS device_category,
    MIN_BY(device.mobileDeviceModel, visitStartTime) AS mobile_device_model,
    MIN_BY(device.mobileDeviceInfo, visitStartTime) AS mobile_device_string,
    MIN_BY(device.operatingSystem, visitStartTime) AS os,
    MIN_BY(device.operatingSystemVersion, visitStartTime) AS os_version,
    MIN_BY(device.language, visitStartTime) AS language,
    MIN_BY(device.browser, visitStartTime) AS browser,
    MIN_BY(device.browserVersion, visitStartTime) AS browser_version,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  WHERE
    -- This table is partitioned, so we only process the data from session_date
    -- To handle late-arriving data, we process 3 days of data each day (re-processing the past 2)
    -- as separate Airflow tasks (or via bqetl backfill, I haven't decided yet)
    --
    -- Here, we need to take data from yesterday, just in case some of our sessions from today
    -- actually started yesterday. If they did, they'll be filtered out in the HAVING clause
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@session_date, INTERVAL 1 DAY))
    -- However, we have data for today that will arrive _tomorrow_! Some inter-day sessions
    -- will be present in two days, with the same ids. A session should never span more
    -- than two days though, see https://sql.telemetry.mozilla.org/queries/95882/source
    -- If one does, our uniqueness check will alert us
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@session_date, INTERVAL 1 DAY))
  GROUP BY
    ga_client_id,
    ga_session_id
  HAVING
    -- Don't include entries from today that started yesterday
    session_date = @session_date
)
SELECT
  * EXCEPT (hits),
  (
    SELECT
      LOGICAL_OR(type = 'EVENT' AND eventInfo.eventAction = 'Firefox Download')
    FROM
      UNNEST(hits)
  ) AS had_download_event,
  (
    SELECT
      MAX_BY(normalize_install_target(eventInfo.eventLabel), hitNumber)
    FROM
      UNNEST(hits)
    WHERE
      type = 'EVENT'
      AND eventInfo.eventAction = 'Firefox Download'
      AND normalize_install_target(eventInfo.eventLabel) IS NOT NULL
  ) AS last_reported_install_target,
  (
    SELECT
      ARRAY_AGG(DISTINCT normalize_install_target(eventInfo.eventLabel))
    FROM
      UNNEST(hits)
    WHERE
      type = 'EVENT'
      AND eventInfo.eventAction = 'Firefox Download'
      AND normalize_install_target(eventInfo.eventLabel) IS NOT NULL
  ) AS all_reported_install_targets,
  (
    SELECT
      MAX_BY(eventInfo.eventLabel, hitNumber)
    FROM
      UNNEST(hits)
    WHERE
      type = 'EVENT'
      AND eventInfo.eventAction = 'Stub Session ID'
      AND mozfun.ga.nullify_string(eventInfo.eventLabel) IS NOT NULL
  ) AS last_reported_stub_session_id,
  (
    SELECT
      ARRAY_AGG(DISTINCT eventInfo.eventLabel)
    FROM
      UNNEST(hits)
    WHERE
      type = 'EVENT'
      AND eventInfo.eventAction = 'Stub Session ID'
      AND mozfun.ga.nullify_string(eventInfo.eventLabel) IS NOT NULL
  ) AS all_reported_stub_session_ids,
  (
    -- Most sessions only have 1 landing screen
    -- https://sql.telemetry.mozilla.org/queries/95884/source
    SELECT
      MIN_BY(appInfo.landingScreenName, hitNumber)
    FROM
      UNNEST(hits)
  ) AS landing_screen,
FROM
  daily_sessions
