CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.sync`
AS SELECT
  * REPLACE(
    `moz-fx-data-shared-prod.udf.normalize_metadata`(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.sync_v4`
