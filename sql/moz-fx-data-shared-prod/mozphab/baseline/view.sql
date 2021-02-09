CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozphab.baseline`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozphab_stable.baseline_v1`
