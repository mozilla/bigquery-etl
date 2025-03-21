-- Query for search_derived.desktop_search_aggregates_for_searchreport_v1
SELECT
  submission_date,
  CASE
    WHEN country IN (
        'US',
        'DE',
        'FR',
        'GB',
        'CA',
        'BR',
        'RU',
        'PL',
        'CN',
        'IN',
        'IT',
        'ES',
        'ID',
        'KE',
        'JP'
      )
      THEN country
    ELSE 'others'
  END AS geo,
  CASE
    WHEN SUBSTR(locale, 0, 2) IN ('en', 'de', 'es', 'fr', 'ru', 'zh', 'pt', 'pl', 'ja', 'it')
      THEN SUBSTR(locale, 0, 2)
    ELSE 'others'
  END AS locale,
  normalized_engine AS engine,
  mozfun.norm.os(os) AS os,
  SPLIT(app_version, '.')[OFFSET(0)] AS app_version,
  SUM(
    client_count
  ) AS dcc, # be careful of double counting for client_id with 1+ engine on the same day
  SUM(sap) AS sap,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(search_with_ads) AS search_with_ads,
  SUM(ad_click) AS ad_click,
  SUM(organic) AS organic,
  SUM(ad_click_organic) AS ad_click_organic,
  SUM(search_with_ads_organic) AS search_with_ads_organic
FROM
  `moz-fx-data-shared-prod.search.search_aggregates`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  geo,
  locale,
  normalized_engine,
  os,
  app_version
