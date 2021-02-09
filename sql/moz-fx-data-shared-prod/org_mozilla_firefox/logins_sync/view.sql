CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.logins_sync`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.logins_sync_v1`
