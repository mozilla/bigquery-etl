CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.migration`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.migration_v1`
