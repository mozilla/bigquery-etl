-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.urlbar_potential_exposure`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    'Firefox' AS normalized_app_name
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_potential_exposure_v1`