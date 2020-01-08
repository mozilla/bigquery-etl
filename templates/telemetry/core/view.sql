CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.core`
AS
WITH unioned AS (
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v2`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v3`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v4`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v5`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v6`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v7`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v8`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v9`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_stable.core_v10`)
  --
SELECT
  * REPLACE(
    `moz-fx-data-shared-prod.udf.normalize_metadata`(metadata) AS metadata)
FROM
  unioned
