CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_usage_jobs_by_org`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_usage_v2`
