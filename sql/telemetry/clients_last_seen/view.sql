CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_last_seen_v1`
