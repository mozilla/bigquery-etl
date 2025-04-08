-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_mozregression.usage`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* EXCEPT (jwe, labeled_rate, text, url) REPLACE(
          STRUCT(
            mozfun.glean.parse_datetime(metrics.datetime.usage_bad_date) AS usage_bad_date,
            metrics.datetime.usage_bad_date AS raw_usage_bad_date,
            mozfun.glean.parse_datetime(metrics.datetime.usage_good_date) AS usage_good_date,
            metrics.datetime.usage_good_date AS raw_usage_good_date,
            mozfun.glean.parse_datetime(metrics.datetime.usage_launch_date) AS usage_launch_date,
            metrics.datetime.usage_launch_date AS raw_usage_launch_date
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
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.usage_v1`
