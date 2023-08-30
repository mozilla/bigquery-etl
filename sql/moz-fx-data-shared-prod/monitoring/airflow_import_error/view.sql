CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_import_error`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_import_error_v1`
