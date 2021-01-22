CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_etl_scheduled_query_usage`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_etl_scheduled_query_usage_v1`
