CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_all`
AS
SELECT
  'structured' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_structured`
UNION ALL
SELECT
  'stub_installer' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_stub_installer`
UNION ALL
SELECT
  'telemetry' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_telemetry`
