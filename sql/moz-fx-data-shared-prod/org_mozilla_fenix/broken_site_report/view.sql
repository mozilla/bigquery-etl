-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.broken_site_report`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (SELECT AS STRUCT metrics.*, metrics.text2 AS text, metrics.url2 AS url) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.broken_site_report_v1`
