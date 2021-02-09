CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_lockbox.history_sync`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_stable.history_sync_v1`
