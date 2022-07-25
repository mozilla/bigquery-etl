CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.newtab_interactions`
AS
SELECT
   * EXCEPT (search_engine),
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(search_engine) AS search_engine,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.newtab_interactions_v1`
