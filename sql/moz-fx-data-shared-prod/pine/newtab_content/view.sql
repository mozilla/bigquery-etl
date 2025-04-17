-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.newtab_content`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
FROM
  `moz-fx-data-shared-prod.pine_stable.newtab_content_v1`
