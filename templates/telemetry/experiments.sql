CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiments`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.experiments_v1`
