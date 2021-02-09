CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozphab.usage`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozphab_stable.usage_v1`
