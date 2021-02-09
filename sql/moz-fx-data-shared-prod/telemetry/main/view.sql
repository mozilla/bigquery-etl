CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.main`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v4`
