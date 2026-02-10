CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
AS
SELECT
  submission_date AS submission_date_s3,
  * EXCEPT (normalized_engine),
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8`
