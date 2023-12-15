CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.mobile_search_aggregates`
AS
SELECT
  * EXCEPT (normalized_engine, normalized_app_name),
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
  `mozfun.mobile_search.normalize_app_name`(app_name, os).normalized_app_name AS normalized_app_name,
  search_count AS sap,
  `mozfun.mobile_search.normalize_app_name`(app_name, os).normalized_app_name_os
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_aggregates_v1`
WHERE
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
