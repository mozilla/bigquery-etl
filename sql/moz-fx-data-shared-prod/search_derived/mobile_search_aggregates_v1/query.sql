SELECT
  submission_date,
  engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version,
  COUNT(*) AS client_count,
  SUM(search_count) AS search_count,
  SUM(organic) AS organic,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(ad_click) AS ad_click,
  SUM(ad_click_organic) AS ad_click_organic,
  SUM(search_with_ads) AS search_with_ads,
  SUM(search_with_ads_organic) AS search_with_ads_organic,
  SUM(unknown) AS unknown,
  CAST(NULL AS string) normalized_engine,
  CASE
    WHEN default_search_engine LIKE '%google%'
      THEN "Google"
    WHEN default_search_engine LIKE '%bing%'
      THEN "Bing"
    WHEN default_search_engine LIKE '%ddg%'
      OR default_search_engine LIKE '%duckduckgo%'
      THEN "DuckDuckGo"
    ELSE NULL
  END AS normalized_default_search_engine,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name_os AS normalized_app_name_os
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
WHERE
  submission_date = @submission_date
  AND submission_date <= '2024-07-31'
  AND engine IS NOT NULL
  AND (
    app_name NOT IN (
      'Fennec',
      'Focus Android Glean',
      'Klar Android Glean',
      'Focus iOS Glean',
      'Klar iOS Glean',
      'Focus',
      'Klar'
    )
    OR (
      app_name = 'Fennec'
      AND (
        os != 'iOS'
        OR submission_date < '2023-01-01'
        OR mozfun.norm.truncate_version(app_version, 'major') >= 28
      )
    )
    OR (
      app_name IN ('Focus Android Glean', 'Klar Android Glean', 'Focus iOS Glean', 'Klar iOS Glean')
      AND submission_date >= '2023-01-01'
    )
    OR (app_name IN ('Focus', 'Klar') AND submission_date < '2023-01-01')
  )
GROUP BY
  submission_date,
  engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version
UNION ALL
SELECT
  submission_date,
  engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version,
  COUNT(*) AS client_count,
  SUM(search_count) AS search_count,
  SUM(organic) AS organic,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(ad_click) AS ad_click,
  SUM(ad_click_organic) AS ad_click_organic,
  SUM(search_with_ads) AS search_with_ads,
  SUM(search_with_ads_organic) AS search_with_ads_organic,
  SUM(unknown) AS unknown,
  CAST(NULL AS string) normalized_engine,
  CASE
    WHEN default_search_engine LIKE '%google%'
      THEN "Google"
    WHEN default_search_engine LIKE '%bing%'
      THEN "Bing"
    WHEN default_search_engine LIKE '%ddg%'
      OR default_search_engine LIKE '%duckduckgo%'
      THEN "DuckDuckGo"
    ELSE NULL
  END AS normalized_default_search_engine,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name_os AS normalized_app_name_os
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2`
WHERE
  submission_date = @submission_date
  AND submission_date > '2024-07-31'
  AND engine IS NOT NULL
  AND (
    app_name NOT IN (
      'Fennec',
      'Focus Android Glean',
      'Klar Android Glean',
      'Focus iOS Glean',
      'Klar iOS Glean',
      'Focus',
      'Klar'
    )
    OR (
      app_name = 'Fennec'
      AND (
        os != 'iOS'
        OR submission_date < '2023-01-01'
        OR mozfun.norm.truncate_version(app_version, 'major') >= 28
      )
    )
    OR (
      app_name IN ('Focus Android Glean', 'Klar Android Glean', 'Focus iOS Glean', 'Klar iOS Glean')
      AND submission_date >= '2023-01-01'
    )
    OR (app_name IN ('Focus', 'Klar') AND submission_date < '2023-01-01')
  )
GROUP BY
  submission_date,
  engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version
