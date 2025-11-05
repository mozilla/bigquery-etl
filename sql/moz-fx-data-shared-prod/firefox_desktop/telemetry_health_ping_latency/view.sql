CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.telemetry_health_ping_latency`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.telemetry_health_ping_latency_v1`
