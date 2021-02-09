CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_mozregression.usage`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.usage_v1`
