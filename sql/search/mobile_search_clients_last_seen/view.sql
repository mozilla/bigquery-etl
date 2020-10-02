CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.mobile_search_clients_last_seen`
AS
SELECT
  * EXCEPT (engine_searches, total_searches),
  `moz-fx-data-shared-prod`.udf.normalize_monthly_searches(engine_searches) AS engine_searches,
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_last_seen_v1`
