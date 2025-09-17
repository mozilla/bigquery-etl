-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.metrics`
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
              metrics.datetime.app_last_opened_as_default_browser
            ) AS app_last_opened_as_default_browser,
            metrics.datetime.app_last_opened_as_default_browser AS raw_app_last_opened_as_default_browser,
            mozfun.glean.parse_datetime(
              metrics.datetime.glean_validation_first_run_hour
            ) AS glean_validation_first_run_hour,
            metrics.datetime.glean_validation_first_run_hour AS raw_glean_validation_first_run_hour,
            mozfun.glean.parse_datetime(metrics.datetime.termsofuse_date) AS termsofuse_date,
            metrics.datetime.termsofuse_date AS raw_termsofuse_date,
            mozfun.glean.parse_datetime(
              metrics.datetime.user_terms_of_use_date_accepted
            ) AS user_terms_of_use_date_accepted,
            metrics.datetime.user_terms_of_use_date_accepted AS raw_user_terms_of_use_date_accepted
          ) AS datetime
        )
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
