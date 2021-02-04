CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.searchvolextra`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.searchvolextra_v4`
