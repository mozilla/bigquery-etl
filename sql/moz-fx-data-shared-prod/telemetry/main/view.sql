-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.main`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    `moz-fx-data-shared-prod`.udf.normalize_main_payload(payload) AS payload
  )
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v5`
