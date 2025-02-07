CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
AS
-- We exclude the payload field for views that are accessible to
-- most users.
SELECT
  'structured' AS pipeline_family,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.structured`
UNION ALL
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
