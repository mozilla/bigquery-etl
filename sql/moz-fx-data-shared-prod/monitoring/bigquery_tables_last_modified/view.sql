CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_tables_last_modified`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_tables_last_modified_v1`
