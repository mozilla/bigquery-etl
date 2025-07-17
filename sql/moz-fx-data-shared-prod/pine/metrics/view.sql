-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.metrics`
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
              metrics.datetime.background_update_time_last_update_scheduled
            ) AS background_update_time_last_update_scheduled,
            metrics.datetime.background_update_time_last_update_scheduled AS raw_background_update_time_last_update_scheduled,
            mozfun.glean.parse_datetime(
              metrics.datetime.blocklist_last_modified_rs_addons_mblf
            ) AS blocklist_last_modified_rs_addons_mblf,
            metrics.datetime.blocklist_last_modified_rs_addons_mblf AS raw_blocklist_last_modified_rs_addons_mblf,
            mozfun.glean.parse_datetime(
              metrics.datetime.blocklist_mlbf_generation_time
            ) AS blocklist_mlbf_generation_time,
            metrics.datetime.blocklist_mlbf_generation_time AS raw_blocklist_mlbf_generation_time,
            mozfun.glean.parse_datetime(
              metrics.datetime.blocklist_mlbf_softblocks_generation_time
            ) AS blocklist_mlbf_softblocks_generation_time,
            metrics.datetime.blocklist_mlbf_softblocks_generation_time AS raw_blocklist_mlbf_softblocks_generation_time,
            mozfun.glean.parse_datetime(
              metrics.datetime.blocklist_mlbf_stash_time_newest
            ) AS blocklist_mlbf_stash_time_newest,
            metrics.datetime.blocklist_mlbf_stash_time_newest AS raw_blocklist_mlbf_stash_time_newest,
            mozfun.glean.parse_datetime(
              metrics.datetime.blocklist_mlbf_stash_time_oldest
            ) AS blocklist_mlbf_stash_time_oldest,
            metrics.datetime.blocklist_mlbf_stash_time_oldest AS raw_blocklist_mlbf_stash_time_oldest,
            mozfun.glean.parse_datetime(
              metrics.datetime.glean_validation_first_run_hour
            ) AS glean_validation_first_run_hour,
            metrics.datetime.glean_validation_first_run_hour AS raw_glean_validation_first_run_hour,
            mozfun.glean.parse_datetime(
              metrics.datetime.legacy_telemetry_session_start_date
            ) AS legacy_telemetry_session_start_date,
            metrics.datetime.legacy_telemetry_session_start_date AS raw_legacy_telemetry_session_start_date,
            mozfun.glean.parse_datetime(metrics.datetime.termsofuse_date) AS termsofuse_date,
            metrics.datetime.termsofuse_date AS raw_termsofuse_date
          ) AS datetime
        ),
        metrics.text2 AS text,
        metrics.url2 AS url
    ) AS metrics,
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
  `moz-fx-data-shared-prod.pine_stable.metrics_v1`
