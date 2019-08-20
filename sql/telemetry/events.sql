CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.events`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.events_v1`
