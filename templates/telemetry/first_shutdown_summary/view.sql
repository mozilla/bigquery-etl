CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.first_shutdown_summary`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry.first_shutdown_summary_v4`
