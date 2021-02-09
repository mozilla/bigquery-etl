CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.metrics_v1`
