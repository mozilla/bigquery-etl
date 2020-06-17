CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_aggregates`
AS
SELECT
  submission_date AS submission_date_s3,
  * EXCEPT (normalized_engine),
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
FROM
  `moz-fx-data-shared-prod.search_derived.search_aggregates_v8`
