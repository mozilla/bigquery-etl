CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
AS
SELECT
  'contextual_services' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_contextual_services`
UNION ALL
SELECT
  'structured' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_structured`
UNION ALL
SELECT
  'stub_installer' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_stub_installer`
UNION ALL
SELECT
  'telemetry' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_telemetry`
UNION ALL
SELECT
  'tls_error_reports' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_tls_error_reports`
