CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.space_ship_ready`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.burnham_stable.space_ship_ready_v1`
