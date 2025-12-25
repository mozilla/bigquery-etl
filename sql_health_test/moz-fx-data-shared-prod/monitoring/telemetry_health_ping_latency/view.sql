CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.telemetry_health_ping_latency`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.telemetry_health_ping_latency_v1`
