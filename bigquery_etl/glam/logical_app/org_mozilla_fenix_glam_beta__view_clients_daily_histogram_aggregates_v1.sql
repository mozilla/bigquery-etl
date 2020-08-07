CREATE OR REPLACE VIEW
  glam_etl.org_mozilla_fenix_glam_beta__view_clients_daily_histogram_aggregates_v1
AS
SELECT
  *
FROM
  glam_etl.org_mozilla_fenix__view_clients_daily_histogram_aggregates_v1
WHERE
  `moz-fx-data-shared-prod`.udf.fenix_build_to_datetime(app_build_id) < date "2020-07-03"
UNION
SELECT
  *
FROM
  glam_etl.org_mozilla_firefox_beta__view_clients_daily_histogram_aggregates_v1
