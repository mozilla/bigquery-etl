CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.crash_summary`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.crash_summary_v2`
