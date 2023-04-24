CREATE TEMP FUNCTION normalize_browser(browser STRING) AS (
  CASE
    WHEN `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser)
      THEN 'Firefox'
    WHEN browser IN ('Internet Explorer')
      THEN 'MSIE'
    WHEN browser IN ('Edge')
      THEN 'Edge'
    WHEN browser IN ('Chrome')
      THEN 'Chrome'
    WHEN browser IN ('Safari')
      THEN 'Safari'
    WHEN browser IN ('(not set)')
      THEN NULL
    WHEN browser IS NULL
      THEN NULL
    ELSE 'Other'
  END
);

-- Unnest all the hits for all  sessions, one row per hit.
-- Different hit.type values (EVENT | PAGE)
-- and hit.eventInfo.eventAction('Firefox Download'| 'Stub Session ID') are
-- extracted in the following CTEs
-- Those extracted fields are joined to the GA session level data, one session per day.
WITH all_hits AS (
  SELECT
    clientId AS client_id,
    visitId AS visit_id,
    hit.appInfo.landingScreenName AS landing_page,
    hit.page.pagePath AS pagepath,
    CASE
      WHEN (
          hit.type = 'EVENT'
          AND hit.eventInfo.eventAction = 'Firefox Download'
          AND hit.eventInfo.eventCategory IS NOT NULL
          AND hit.eventInfo.eventLabel LIKE 'Firefox for Desktop%'
        )
        THEN TRUE
      ELSE FALSE
    END AS has_ga_download_event,
    hit.type AS hit_type,
    CASE
      WHEN (hit.type = 'EVENT' AND hit.eventInfo.eventAction = 'Stub Session ID')
        THEN hit.eventInfo.eventLabel
      ELSE NULL
    END AS download_session_id
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*` AS ga
  CROSS JOIN
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@download_date, INTERVAL 2 DAY))
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@download_date, INTERVAL 1 DAY))
),
page_hits AS (
  SELECT
    client_id AS client_id,
    visit_id AS visit_id,
    COUNT(pagePath) AS pageviews,
    COUNT(DISTINCT pagePath) AS unique_pageviews,
  FROM
    all_hits
  WHERE
    hit_type = 'PAGE'
  GROUP BY
    client_id,
    visit_id
),
extract_download_session_id AS (
 select
    client_id AS client_id,
    visit_id AS visit_id,
    download_session_id
  FROM
    all_hits
  WHERE
     hit_type = 'EVENT'
     and download_session_id is not NULL
),
event_hits AS (
  SELECT
    client_id AS client_id,
    visit_id AS visit_id,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(landing_page)) AS landing_page,
    LOGICAL_OR(has_ga_download_event) AS has_ga_download_event
  FROM
    all_hits
  WHERE
     hit_type = 'EVENT'
  GROUP BY
    client_id,
    visit_id
    -- download_session_id  cannot group by download_session_id here since not every hit will have download_session_id
),
ga_session_dimensions AS (
  SELECT
    clientId AS client_id,
    visitId AS visit_id,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(geoNetwork.country)) AS country,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(trafficSource.adContent)) AS ad_content,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(trafficSource.campaign)) AS campaign,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(trafficSource.medium)) AS medium,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(trafficSource.source)) AS source,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device.deviceCategory)) AS device_category,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device.operatingSystem)) AS os,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device.browser)) AS browser,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device.browserVersion)) AS browser_version,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device.language)) AS `language`,
    IFNULL(mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(totals.timeOnSite)), 0) AS time_on_site
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*` AS ga
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@download_date, INTERVAL 2 DAY))
    AND FORMAT_DATE('%Y%m%d', DATE_ADD(@download_date, INTERVAL 1 DAY))
  GROUP BY
    client_id,
    visit_id
),
ga_sessions_with_hits_fields AS (
  SELECT
    * EXCEPT (pageviews, unique_pageviews),
    IFNULL(pageviews, 0) AS pageviews,
    IFNULL(unique_pageviews, 0) AS unique_pageviews,
  FROM
    ga_session_dimensions ga
  LEFT JOIN
    page_hits p
  USING
    (client_id, visit_id)
  JOIN
    event_hits
  USING
    (client_id, visit_id)
  LEFT JOIN
    extract_download_session_id
  USING
    (client_id, visit_id)
),

-- Extract all the download rows, de-duping and tracking number of duplicates per download token.
stub_downloads AS (
  SELECT
    s.jsonPayload.fields.visit_id AS stub_visit_id,
    jsonPayload.fields.session_id AS stub_download_session_id,
    s.jsonPayload.fields.dltoken AS dltoken,
    (COUNT(*) - 1) AS count_dltoken_duplicates,
    DATE(@download_date) AS download_date
  FROM
    `moz-fx-stubattribut-prod-32a5.stubattribution_prod.stdout` s
  WHERE
    DATE(s.timestamp) = @download_date
    AND s.jsonPayload.fields.log_type = 'download_started'
  GROUP BY
    stub_visit_id,
    stub_download_session_id,
    dltoken
),

multiple_downloads_in_session AS (
  SELECT
    stub_visit_id,
    stub_download_session_id,
    CASE
      WHEN (COUNT(*) > 1)
        THEN TRUE
      ELSE FALSE
    END AS additional_download_occurred
  FROM
    stub_downloads
  GROUP BY
    stub_visit_id,
    stub_download_session_id
),

stub_downloads_with_download_tracking AS (
  SELECT
    s1.stub_visit_id,
    s1.stub_download_session_id,
    dltoken,
    count_dltoken_duplicates,
    additional_download_occurred,
    download_date
  FROM
    stub_downloads s1
  JOIN
    multiple_downloads_in_session s2
  ON (
    s1.stub_visit_id = s2.stub_visit_id
    AND
    IFNULL(s1.stub_download_session_id,"null")=IFNULL(s2.stub_download_session_id, "null")
  )
),
-- This will drop all the ga_sessions w/o a DLtoken but keep DLtoken without a GA session.
-- This will also result in multiple rows as the ga.client_id is not unique for the day
-- since this visit_id is missing from the stub.
-- The join must use client_id/stub_visit_id and download_session_id/stub_download_session_id
downloads_with_ga_session AS (
  SELECT
    gs.client_id AS client_id,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(country)) AS country,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(ad_content)) AS ad_content,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(campaign)) AS campaign,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(medium)) AS medium,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(source)) AS source,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device_category)) AS device_category,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(os)) AS os,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(browser)) AS browser,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(browser_version)) AS browser_version,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(`language`)) AS `language`,
    s.stub_visit_id AS stub_visit_id,
    s.stub_download_session_id AS stub_download_session_id,
    s.dltoken AS dltoken,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(landing_page)) AS landing_page,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(pageviews)) AS pageviews,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(unique_pageviews)) AS unique_pageviews,
    LOGICAL_OR(
      has_ga_download_event
    ) AS has_ga_download_event,
    mozfun.stats.mode_last_retain_nulls(
      ARRAY_AGG(count_dltoken_duplicates)
    ) AS count_dltoken_duplicates,
    LOGICAL_OR(additional_download_occurred) AS additional_download_occurred,
    COUNT(*) AS nrows,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(s.download_date)) AS download_date,
    mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(time_on_site)) AS time_on_site
  FROM
    ga_sessions_with_hits_fields gs
  RIGHT JOIN
    stub_downloads_with_download_tracking s
  ON
    (gs.client_id = s.stub_visit_id AND gs.download_session_id = s.stub_download_session_id)
  GROUP BY
    gs.client_id,
    dltoken,
    stub_visit_id,
    stub_download_session_id
),
-- This table is the result of joining the stub downlaod data with the GA rows
-- using the client_id and the custom session_download_id
v2_table AS (
SELECT
  dltoken,
  download_date,
  IF(nrows <= 1, time_on_site, NULL)  time_on_site,
  IF(nrows <= 1, ad_content, NULL)  ad_content,
  IF(nrows <= 1, campaign, NULL)  campaign,
  IF(nrows <= 1, medium, NULL)  medium,
  IF(nrows <= 1, source, NULL)  source,
  IF(nrows <= 1, landing_page, NULL)  landing_page,
  IF(nrows <= 1, country, NULL)  country,
  IF(nrows <= 1, cn.code, NULL)  normalized_country_code,
  IF(nrows <= 1, device_category, NULL)  device_category,
  IF(nrows <= 1, os, NULL) os,
  CASE
    WHEN nrows > 1
      THEN NULL
    WHEN os IS NULL
      THEN NULL
    WHEN os LIKE 'Macintosh%'
      THEN 'Mac'  -- these values are coming from GA.
    ELSE mozfun.norm.os(os)
  END
  normalized_os,
  IF(nrows <= 1, browser, NULL) browser,
  IF(nrows <= 1, normalize_browser(browser), NULL) normalized_browser,
  IF(nrows <= 1, browser_version, NULL) browser_version,
 -- only setting browser major version since that is the only value used in
 -- moz-fx-data-shared-prod.firefox_installer.install

  IF(nrows <= 1, CAST(mozfun.norm.extract_version(browser_version, 'major') AS INTEGER), NULL) browser_major_version,
  IF(nrows <= 1, `language`, NULL) `language`,
  IF(nrows <= 1, pageviews, NULL) pageviews,
  IF(nrows <= 1, unique_pageviews, NULL) unique_pageviews,
  IF(nrows <= 1, has_ga_download_event, NULL) has_ga_download_event,
  count_dltoken_duplicates,
  additional_download_occurred,
  CASE
    WHEN stub_visit_id IS NULL
      OR stub_download_session_id IS NULL
      THEN 'DOWNLOAD_SESSION_ID_NULL'
    WHEN stub_visit_id = ''
      OR stub_download_session_id = ''
      THEN 'DOWNLOAD_SESSION_ID_EMPTY'
    WHEN stub_visit_id = '(not set)'
      OR stub_download_session_id = '(not set)'
      THEN 'DOWNLOAD_SESSION_ID_VALUE_NOT_SET'
    WHEN stub_visit_id = 'something'
      OR stub_download_session_id = 'something'
      THEN 'DOWNLOAD_SESSION_ID_VALUE_SOMETHING'
    WHEN client_id IS NULL
      THEN 'MISSING_GA_CLIENT'
    WHEN dltoken IS NULL
      THEN 'MISSING_DL_TOKEN'
    WHEN nrows > 1
      THEN 'GA_UNRESOLVABLE'
    ELSE NULL
  END
  `exception`
FROM
  downloads_with_ga_session
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
ON
  cn.name = country
),

-- Some of the joins for v2_table as not successful due to the GA data not including
-- the download_session_id.  The downloads which are unable to match to a GA session
-- are set to exception='MISSING_GA_CLIENT'
-- Those dltokens are re-processed using the V1 logic (loin using only the client_id)
extract_dltoken_missing_ga_client AS (
  SELECT s.stub_visit_id, v2.dltoken, v2.count_dltoken_duplicates,  v2.additional_download_occurred,  v2.download_date
  FROM v2_table v2
  JOIN stub_downloads_with_download_tracking s
  ON (s.dltoken = v2.dltoken)
  WHERE exception = 'MISSING_GA_CLIENT'
),
v1_downloads_with_ga_session AS (
  SELECT
  gs.client_id,
  dltoken,
  stub_visit_id,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(country)) AS country,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(ad_content)) AS ad_content,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(campaign)) AS campaign,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(medium)) AS medium,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(source)) AS source,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(device_category)) AS device_category,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(os)) AS os,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(browser)) AS browser,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(browser_version)) AS browser_version,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(`language`)) AS `language`,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(landing_page)) AS landing_page,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(pageviews)) AS pageviews,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(unique_pageviews)) AS unique_pageviews,
  LOGICAL_OR(
    has_ga_download_event
  ) AS has_ga_download_event,
  mozfun.stats.mode_last_retain_nulls(
    ARRAY_AGG(count_dltoken_duplicates)
  ) AS count_dltoken_duplicates,
  LOGICAL_OR(additional_download_occurred) AS additional_download_occurred,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(s.download_date)) AS download_date,
  mozfun.stats.mode_last_retain_nulls(ARRAY_AGG(time_on_site)) AS time_on_site,
  COUNT(*) AS nrows
  FROM
    ga_sessions_with_hits_fields gs
  RIGHT JOIN
    extract_dltoken_missing_ga_client s
  ON
    gs.client_id = s.stub_visit_id
  GROUP BY
    gs.client_id, stub_visit_id, dltoken

),
v1_table AS (
SELECT
  dltoken,
  download_date,
  IF(nrows <= 1, time_on_site, NULL)  time_on_site,
  IF(nrows <= 1, ad_content, NULL)  ad_content,
  IF(nrows <= 1, campaign, NULL)  campaign,
  IF(nrows <= 1, medium, NULL)  medium,
  IF(nrows <= 1, source, NULL)  source,
  IF(nrows <= 1, landing_page, NULL)  landing_page,
  IF(nrows <= 1, country, NULL)  country,
  IF(nrows <= 1, cn.code, NULL)  normalized_country_code,
  IF(nrows <= 1, device_category, NULL)  device_category,
  IF(nrows <= 1, os, NULL) os,
  CASE
    WHEN nrows > 1
      THEN NULL
    WHEN os IS NULL
      THEN NULL
    WHEN os LIKE 'Macintosh%'
      THEN 'Mac'  -- these values are coming from GA.
    ELSE mozfun.norm.os(os)
  END
  normalized_os,
  IF(nrows <= 1, browser, NULL) browser,
  IF(nrows <= 1, normalize_browser(browser), NULL) normalized_browser,
  IF(nrows <= 1, browser_version, NULL) browser_version,
 -- only setting browser major version since that is the only value used in
 -- moz-fx-data-shared-prod.firefox_installer.install

  IF(nrows <= 1, CAST(mozfun.norm.extract_version(browser_version, 'major') AS INTEGER), NULL) browser_major_version,
  IF(nrows <= 1, `language`, NULL) `language`,
  IF(nrows <= 1, pageviews, NULL) pageviews,
  IF(nrows <= 1, unique_pageviews, NULL) unique_pageviews,
  IF(nrows <= 1, has_ga_download_event, NULL) has_ga_download_event,
  count_dltoken_duplicates,
  additional_download_occurred,
  CASE
    WHEN stub_visit_id IS NULL
      THEN 'DOWNLOAD_SESSION_ID_NULL'
    WHEN stub_visit_id = ''
      THEN 'DOWNLOAD_SESSION_ID_EMPTY'
    WHEN stub_visit_id = '(not set)'
      THEN 'DOWNLOAD_SESSION_ID_VALUE_NOT_SET'
    WHEN stub_visit_id = 'something'
      THEN 'DOWNLOAD_SESSION_ID_VALUE_SOMETHING'
    WHEN client_id IS NULL
      THEN 'MISSING_GA_CLIENT'
    WHEN dltoken IS NULL
      THEN 'MISSING_DL_TOKEN'
    WHEN nrows > 1
      THEN 'GA_UNRESOLVABLE'
    ELSE NULL
  END
  `exception`
FROM
  v1_downloads_with_ga_session
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
ON
  cn.name = country
)

SELECT *
FROM v1_table
UNION ALL
SELECT *
FROM v2_table
WHERE exception != 'MISSING_GA_CLIENT' OR exception is NULL