-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.font_list`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (SELECT AS STRUCT metrics.*, metrics.text2 AS text) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.font_list_v1`
