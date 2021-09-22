CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_all`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_structured`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_stub_installer`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_telemetry`
