CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.crash_aggregates`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.crash_aggregates_v1`
