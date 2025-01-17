-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.usage_deletion_request`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.usage_deletion_request_v1`
