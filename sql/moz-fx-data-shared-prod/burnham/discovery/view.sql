CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.discovery`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.burnham_stable.discovery_v1`
