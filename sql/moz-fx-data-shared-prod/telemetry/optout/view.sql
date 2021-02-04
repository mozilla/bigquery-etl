CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.optout`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.optout_v4`
