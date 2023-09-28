CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.first_shutdown`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    `moz-fx-data-shared-prod`.udf.normalize_main_payload(payload) AS payload
  )
FROM
  `moz-fx-data-shared-prod.telemetry_stable.first_shutdown_v4`
