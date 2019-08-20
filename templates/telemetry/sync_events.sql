CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.sync_events`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.sync_events_v1`
