CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.burnham.deletion_request`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.burnham_stable.deletion_request_v1`
