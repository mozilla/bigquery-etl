CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_cert_error_ssl_handshake_failure_rate_by_os`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_cert_error_ssl_handshake_failure_rate_by_os_v1`
