CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
AS
-- payload_bytes_error.structured is restricted from most users,
-- so we have to reference an authorized view.
SELECT
  'structured' AS pipeline_family,
  *
FROM
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_structured`
UNION ALL
-- All other error tables are accessible, but we need to exclude
-- the payload field to match the schema of the authorized view above.
SELECT
  'stub_installer' AS pipeline_family,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.stub_installer`
UNION ALL
SELECT
  'telemetry' AS pipeline_family,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.telemetry`
