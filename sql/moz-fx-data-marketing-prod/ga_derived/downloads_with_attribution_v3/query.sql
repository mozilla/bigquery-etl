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

WITH
-- Extract all the download rows, de-duping and tracking number of duplicates per download token.
-- Also filter out null/empty strings for stub_visit_ids and stub_download_session_ids
stub_downloads AS (
  SELECT
    stub.jsonPayload.fields.visit_id AS stub_visit_id,
    stub.jsonPayload.fields.session_id AS stub_download_session_id,
    stub.jsonPayload.fields.dltoken AS dltoken,
    (COUNT(*) - 1) AS count_dltoken_duplicates,
    -- DATE(@download_date) AS download_date
    DATE("2024-02-14") AS download_date
  FROM
    `moz-fx-stubattribut-prod-32a5.stubattribution_prod.stdout` AS stub
  WHERE
    -- DATE(stub.timestamp) = @download_date
    DATE(stub.timestamp) = "2024-02-15"
    -- AND DATE(stub.timestamp) <= "2024-02-20"
    AND stub.jsonPayload.fields.log_type = 'download_started'
  GROUP BY
    stub_visit_id,
    stub_download_session_id,
    dltoken
),

multiple_downloads_in_session AS (
  SELECT
    stub_visit_id,
    stub_download_session_id,
    IF(COUNT(*) > 1, TRUE, FALSE) AS additional_download_occurred
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
      AND IFNULL(s1.stub_download_session_id, "null") = IFNULL(s2.stub_download_session_id, "null")
    )

),

-- Extract all the stub_session_ids from GA
stub_session_ids AS (
  SELECT DISTINCT
    user_pseudo_id AS full_visitor_id,
    CAST(((SELECT `value` FROM UNNEST(event_params) WHERE key = 'id' LIMIT 1).int_value) AS STRING) AS stub_session_id
  FROM `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE event_name = 'stub_session_set'
    -- will need to update this
    AND _TABLE_SUFFIX
        BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB("2024-02-15", INTERVAL 2 DAY))
    AND FORMAT_DATE('%Y%m%d', DATE_ADD("2024-02-15", INTERVAL 1 DAY))
  ),

-- join stub_download_session_ids with ga_stub_session_ids
stub_download_ids_ga_session_ids AS (
SELECT sd.stub_visit_id,
sd.stub_download_session_id,
sd.dltoken,
sd.count_dltoken_duplicates,
sd.additional_download_occurred,
sd.download_date,
ssi.full_visitor_id
FROM stub_downloads_with_download_tracking sd
LEFT JOIN stub_session_ids ssi
ON sd.stub_download_session_id = ssi.stub_session_id
)
,

ga_sessions_time_on_site AS (
  SELECT CONCAT(ga_client_id, "-", ga_session_id) AS visit_identifier,
    time_on_site,
    had_download_event
  FROM `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2`
  WHERE session_date = '2024-02-14'
  AND had_download_event IS TRUE
),

page_hits AS (
SELECT
  ph.full_visitor_id,
  ph.visit_identifier,
  ph.date AS submission_date,
  ph.country,
  ph.ad_content,
  ph.campaign,
  ph.medium,
  ph.source,
  ph.device_category,
  ph.operating_system AS os,
  ph.browser,
  ph.browser_version,
  ph.language,
  ph.page_path,
  SUM(CASE WHEN ph.hit_type = 'PAGE' then 1 else 0 end) AS page_hits,
  COUNT(distinct(CASE WHEN ph.hit_type = 'PAGE' THEN ph.page_path ELSE NULL END)) AS unique_page_hits,
  MAX(CASE WHEN ph.is_entrance is true then ph.page_path ELSE NULL END) as landing_page
FROM `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v2`  ph
WHERE date = "2024-02-14"
  GROUP BY
  ph.full_visitor_id,
  ph.visit_identifier,
  ph.date,
  ph.country,
  ph.ad_content,
  ph.campaign,
  ph.medium,
  ph.source,
  ph.device_category,
  ph.operating_system,
  ph.browser,
  ph.browser_version,
  ph.language,
  ph.page_path
)
,
-- join stub_sessions with ga_sessions using full_visitor_id
downloads_with_ga_sessions AS (
  SELECT
  sd.stub_visit_id,
  sd.stub_download_session_id,
  sd.dltoken,
  sd.count_dltoken_duplicates,
  sd.additional_download_occurred,
  sd.download_date,
  sd.full_visitor_id,
  ph.full_visitor_id as full_visitor_id_check,
  ph.visit_identifier,
  ph.submission_date,
  ph.country,
  ph.ad_content,
  ph.campaign,
  ph.medium,
  ph.source,
  ph.device_category,
  ph.os,
  ph.browser,
  ph.browser_version,
  ph.language,
  ph.page_path,
  gav.time_on_site,
  ph.page_hits,
  ph.unique_page_hits,
  ph.landing_page,
  gav.had_download_event,
 FROM stub_download_ids_ga_session_ids sd
 LEFT JOIN page_hits ph ON
 ph.full_visitor_id = sd.full_visitor_id
 AND sd.download_date = ph.submission_date
 LEFT JOIN ga_sessions_time_on_site gav
  ON gav.visit_identifier = ph.visit_identifier
)

SELECT
  dgs.dltoken,
  dgs.time_on_site,
  dgs.ad_content,
  dgs.campaign,
  dgs.medium,
  dgs.source,
  dgs.landing_page,
  dgs.country,
  cn.code AS normalized_country_code,
  dgs.device_category,
  dgs.os,
  CASE
    WHEN dgs.os IS NULL
      THEN NULL
    WHEN dgs.os LIKE 'Macintosh%'
      THEN 'Mac'  -- these values are coming from GA.
    ELSE mozfun.norm.os(os)
  END AS normalized_os,
  dgs.browser,
  normalize(dgs.browser) AS normalized_browser,
  dgs.browser_version,
  CAST(mozfun.norm.extract_version(browser_version, 'major') AS INTEGER) AS browser_major_version,
  dgs.language,
  dgs.page_hits AS pageviews,
  dgs.unique_page_hits AS unique_pageviews,
  dgs.had_download_event AS has_ga_download_event,
  dgs.count_dltoken_duplicates,
  dgs.additional_download_occurred,
  dgs.download_date
FROM downloads_with_ga_sessions dgs
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` AS cn
  ON cn.name = country
