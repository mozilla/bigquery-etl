CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_contextual_services`
AS
SELECT
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.contextual_services`
