-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_klar.captcha_detection`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info
  ),
  mozfun.norm.extract_version(client_info.app_display_version, 'major') AS app_version_major,
  mozfun.norm.extract_version(client_info.app_display_version, 'minor') AS app_version_minor,
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_stable.captcha_detection_v1`
