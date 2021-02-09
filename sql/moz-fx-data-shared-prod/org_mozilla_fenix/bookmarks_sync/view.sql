CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.bookmarks_sync`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.bookmarks_sync_v1`
