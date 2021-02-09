CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.burnham_stable.metrics_v1`
