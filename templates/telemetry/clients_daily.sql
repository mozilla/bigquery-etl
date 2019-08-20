CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_daily_v6`
