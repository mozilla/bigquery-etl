-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.top_sites`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (SELECT AS STRUCT metrics.*, metrics.url2 AS url) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.pine_stable.top_sites_v1`
