CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_stub_installer`
AS
SELECT
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.stub_installer`
