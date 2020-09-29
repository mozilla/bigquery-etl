CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_glam_release__view_clients_daily_histogram_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_firefox__view_clients_daily_histogram_aggregates_v1
)
SELECT
  -- NOTE: app_version is dropped due to a lack of semantic versioning. We opt
  -- to use a build id as a placeholder. See
  -- https://github.com/mozilla/bigquery-etl/issues/1329
  * EXCEPT (channel, app_version),
  app_build_id as app_version,
  "*" AS channel
FROM
  extracted
