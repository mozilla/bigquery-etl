-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.metrics`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* EXCEPT (jwe, labeled_rate, text, url) REPLACE(
          STRUCT(
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
            mozfun.glean.parse_datetime(metrics.datetime.terms_of_use_date) AS terms_of_use_date,
            metrics.datetime.terms_of_use_date AS raw_terms_of_use_date
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
