-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    mozfun.norm.glean_baseline_client_info(client_info, metrics) AS client_info,
    'Firefox' AS normalized_app_name
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
