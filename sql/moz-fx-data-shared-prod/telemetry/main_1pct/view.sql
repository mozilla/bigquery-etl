CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.main_1pct`
AS
SELECT
* REPLACE(
mozfun.norm.metadata(metadata) AS metadata,
`moz-fx-data-shared-prod.udf.normalize_main_payload`(payload) AS payload)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.main_1pct_v1`
