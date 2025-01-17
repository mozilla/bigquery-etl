-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.usage_deletion_request`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
FROM
  `moz-fx-data-shared-prod.pine_stable.usage_deletion_request_v1`
