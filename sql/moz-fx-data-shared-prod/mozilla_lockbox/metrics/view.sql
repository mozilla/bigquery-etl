CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_lockbox.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_stable.metrics_v1`
