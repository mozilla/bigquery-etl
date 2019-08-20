CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_heartbeat_parquet`
AS SELECT * FROM
  `moz-fx-data-derived-datasets.telemetry_derived.telemetry_heartbeat_parquet_v1`
