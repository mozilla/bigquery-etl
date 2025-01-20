-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.quick_suggest_deletion_request`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    'Firefox' AS normalized_app_name
  ),
  mozfun.norm.extract_version(client_info.app_display_version, 'major') AS app_version_major,
  mozfun.norm.extract_version(client_info.app_display_version, 'minor') AS app_version_minor,
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.quick_suggest_deletion_request_v1`
