CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.baseline`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.burnham_stable.baseline_v1`
