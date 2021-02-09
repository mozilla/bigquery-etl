CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.metrics_v1`
