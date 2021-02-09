CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.sync`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.sync_v1`
