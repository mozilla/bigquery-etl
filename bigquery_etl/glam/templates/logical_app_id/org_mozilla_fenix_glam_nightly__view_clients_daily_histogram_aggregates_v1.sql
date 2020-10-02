CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_glam_nightly__view_clients_daily_histogram_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix__view_clients_daily_histogram_aggregates_v1
  WHERE
    mozfun.norm.fenix_app_info('org_mozilla_fenix', app_build_id).channel = 'nightly'
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_nightly__view_clients_daily_histogram_aggregates_v1
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fennec_aurora__view_clients_daily_histogram_aggregates_v1
)
SELECT
  -- NOTE: app_version is dropped due to a lack of semantic versioning. We opt
  -- to use a build id as a placeholder. See
  -- https://github.com/mozilla/bigquery-etl/issues/1329
  * EXCEPT (channel, app_version),
  SAFE_CAST(app_build_id AS INT64) AS app_version,
  "*" AS channel
FROM
  extracted
