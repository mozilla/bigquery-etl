CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.mobile_search_clients_last_seen`
AS
SELECT
  * EXCEPT (engine_searches, total_searches),
  `moz-fx-data-shared-prod`.udf.normalize_monthly_searches(engine_searches) AS engine_searches,
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_last_seen_v1`
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
