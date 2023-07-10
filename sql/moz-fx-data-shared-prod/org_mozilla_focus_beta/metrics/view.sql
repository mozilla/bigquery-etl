-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.metrics`
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
            metrics.datetime.glean_validation_first_run_hour AS raw_glean_validation_first_run_hour
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.metrics_v1`
