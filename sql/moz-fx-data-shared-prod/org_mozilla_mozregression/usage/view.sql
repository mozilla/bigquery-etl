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
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(metrics.datetime.usage_bad_date) AS usage_bad_date,
            metrics.datetime.usage_bad_date AS raw_usage_bad_date,
            mozfun.glean.parse_datetime(metrics.datetime.usage_good_date) AS usage_good_date,
            metrics.datetime.usage_good_date AS raw_usage_good_date,
            mozfun.glean.parse_datetime(metrics.datetime.usage_launch_date) AS usage_launch_date,
            metrics.datetime.usage_launch_date AS raw_usage_launch_date
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.usage_v1`
