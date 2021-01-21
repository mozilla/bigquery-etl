CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_etl_scheduled_queries_cost`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_etl_scheduled_queries_cost_v1`
