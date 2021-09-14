CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_tls_error_reports`
AS
SELECT
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.tls_error_reports`
