CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
AS
SELECT
  * EXCEPT (normalized_engine),
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
  search_count AS sap,
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
WHERE
  app_name NOT IN (
    'Focus Android Glean',
    'Klar Android Glean',
    'Focus iOS Glean',
    'Klar iOS Glean',
    'Focus',
    'Klar'
  )
  OR (
    app_name IN ('Focus Android Glean', 'Klar Android Glean', 'Focus iOS Glean', 'Klar iOS Glean')
    AND submission_date >= '2023-01-01'
  )
  OR (app_name IN ('Focus', 'Klar') AND submission_date < '2023-01-01')
