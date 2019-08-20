CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiments_aggregates`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.experiments_aggregates_v1`
