SELECT
  date,
  'mozilla.org' AS site,
  device_category,
  operating_system,
  `language`,
  page_path AS landing_page,
  page_path_level1 AS locale,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  page_name,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser,
  SUM(entrances) AS sessions,
  SUM(
    IF(NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser), entrances, 0)
  ) AS non_fx_sessions,
  COUNTIF(
    (
      `date` <= '2024-02-16'
      AND event_name = 'product_download'
      AND product_type = 'firefox'
      AND platform_type IN (
        'win',
        'win64',
        'macos',
        'linux64',
        'win64-msi',
        'linux',
        'win-msi',
        'win64-aarch64'
      )
    )
    OR (`date` > '2024-02-16' AND event_name = 'firefox_download')
  ) AS downloads,
  COUNTIF(
    (
      (
        `date` <= '2024-02-16'
        AND event_name = 'product_download'
        AND product_type = 'firefox'
        AND platform_type IN (
          'win',
          'win64',
          'macos',
          'linux64',
          'win64-msi',
          'linux',
          'win-msi',
          'win64-aarch64'
        )
      )
      OR (`date` > '2024-02-16' AND event_name = 'firefox_download')
    )
    AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser)
  ) AS non_fx_downloads,
  COUNTIF(event_name = 'page_view') AS pageviews,
  COUNT(
    DISTINCT(CASE WHEN event_name = 'page_view' THEN page_path ELSE NULL END)
  ) AS unique_pageviews,
  COUNT(
    DISTINCT(CASE WHEN single_page_session IS TRUE THEN visit_identifier ELSE NULL END)
  ) AS single_page_sessions,
  COUNT(DISTINCT(CASE WHEN bounces = 1 THEN visit_identifier ELSE NULL END)) AS bounces,
  SUM(exits) AS exits,
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.www_site_hits_v2`
WHERE
  date = @submission_date
GROUP BY
  date,
  site,
  device_category,
  operating_system,
  `language`,
  landing_page,
  locale,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  page_name,
  visit_identifier,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser
