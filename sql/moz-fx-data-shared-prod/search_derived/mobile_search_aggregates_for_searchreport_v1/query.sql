-- Query for search_derived.mobile_search_aggregates_for_searchreport_v1
SELECT
  submission_date,
  country,
  CASE
    WHEN normalized_app_name IN ('Fenix', 'Firefox Preview')
      THEN normalized_app_name
    WHEN normalized_app_name = 'Fennec'
      AND os = 'Android'
      THEN 'Fennec'
    WHEN normalized_app_name = 'Fennec'
      AND os = 'iOS'
      THEN 'Firefox iOS'
    WHEN normalized_app_name = 'Focus'
      AND os = 'Android'
      THEN 'Focus Android'
    WHEN normalized_app_name = 'Focus'
      AND os = 'iOS'
      THEN 'Focus iOS'
    ELSE 'Other'
  END AS product,
  normalized_engine,
  COUNT(DISTINCT client_id) AS clients,
  COUNT(
    DISTINCT(
      CASE
        WHEN sap > 0
          OR tagged_sap > 0
          OR tagged_follow_on > 0
          THEN client_id
        ELSE NULL
      END
    )
  ) AS search_clients,
  SUM(sap) AS sap,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(ad_click) AS ad_click,
  SUM(search_with_ads) AS search_with_ads,
  SUM(organic) AS organic
FROM
  search.mobile_search_clients_engines_sources_daily
WHERE
  normalized_app_name IN ('Fenix', 'Fennec', 'Firefox Preview', 'Focus')
  AND submission_date = @submission_date
GROUP BY
  1,
  2,
  3,
  4
