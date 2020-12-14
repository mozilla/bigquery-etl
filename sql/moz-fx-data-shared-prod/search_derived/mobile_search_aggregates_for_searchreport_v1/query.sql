-- Query for search_derived.mobile_search_aggregates_for_searchreport_v1
SELECT
  submission_date,
  country,
  CASE
  WHEN
    app_name IN ('Fenix', 'Firefox Preview')
  THEN
    app_name
  WHEN
    app_name = 'Fennec'
    AND os = 'Android'
  THEN
    'Fennec'
  WHEN
    app_name = 'Fennec'
    AND os = 'iOS'
  THEN
    'Firefox iOS'
  WHEN
    app_name = 'Focus'
    AND os = 'Android'
  THEN
    'Focus Android'
  WHEN
    app_name = 'Focus'
    AND os = 'iOS'
  THEN
    'Focus iOS'
  ELSE
    'Other'
  END
  AS product,
  normalized_engine,
  count(DISTINCT client_id) AS clients,
  count(
    DISTINCT(
      CASE
      WHEN
        sap > 0
        OR tagged_sap > 0
        OR tagged_follow_on > 0
      THEN
        client_id
      ELSE
        NULL
      END
    )
  ) AS search_clients,
  sum(sap) AS sap,
  sum(tagged_sap) AS tagged_sap,
  sum(tagged_follow_on) AS tagged_follow_on,
  sum(ad_click) AS ad_click,
  sum(search_with_ads) AS search_with_ads,
  sum(organic) AS organic
FROM
  search.mobile_search_clients_daily
WHERE
  app_name IN ('Fenix', 'Fennec', 'Firefox Preview', 'Focus')
  AND app_name IS NOT NULL
  AND submission_date = @submission_date
GROUP BY
  1,
  2,
  3,
  4
