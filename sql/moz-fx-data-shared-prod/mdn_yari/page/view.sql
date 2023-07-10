-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mdn_yari.page`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (SELECT AS STRUCT metrics.*, metrics.url2 AS url) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.mdn_yari_stable.page_v1`
