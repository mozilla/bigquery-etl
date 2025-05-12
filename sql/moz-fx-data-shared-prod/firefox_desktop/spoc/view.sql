-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.spoc`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.pocket_fetch_timestamp
            ) AS pocket_fetch_timestamp,
            metrics.datetime.pocket_fetch_timestamp AS raw_pocket_fetch_timestamp,
            mozfun.glean.parse_datetime(
              metrics.datetime.pocket_newtab_creation_timestamp
            ) AS pocket_newtab_creation_timestamp,
            metrics.datetime.pocket_newtab_creation_timestamp AS raw_pocket_newtab_creation_timestamp
          ) AS datetime
        ),
        metrics.text2 AS text
    ) AS metrics,
    'Firefox' AS normalized_app_name,
    mozfun.norm.glean_client_info_attribution(
      client_info,
      CAST(NULL AS JSON),
      CAST(NULL AS JSON)
    ) AS client_info
  ),
  mozfun.norm.extract_version(client_info.app_display_version, 'major') AS app_version_major,
  mozfun.norm.extract_version(client_info.app_display_version, 'minor') AS app_version_minor,
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch,
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.spoc_v1`
