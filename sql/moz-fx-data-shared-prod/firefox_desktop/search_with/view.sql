-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.search_with`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (SELECT AS STRUCT metrics.*, metrics.url2 AS url) AS metrics,
    'Firefox' AS normalized_app_name
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.search_with_v1`
