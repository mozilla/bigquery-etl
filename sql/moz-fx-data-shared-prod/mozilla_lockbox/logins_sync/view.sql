-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_lockbox.logins_sync`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info
  )
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_stable.logins_sync_v1`
