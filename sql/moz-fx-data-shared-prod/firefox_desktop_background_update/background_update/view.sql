-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_update.background_update`
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
              metrics.datetime.background_update_targeting_env_current_date
            ) AS background_update_targeting_env_current_date,
            metrics.datetime.background_update_targeting_env_current_date AS raw_background_update_targeting_env_current_date,
            mozfun.glean.parse_datetime(
              metrics.datetime.background_update_targeting_env_profile_age
            ) AS background_update_targeting_env_profile_age,
            metrics.datetime.background_update_targeting_env_profile_age AS raw_background_update_targeting_env_profile_age
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.background_update_v1`
