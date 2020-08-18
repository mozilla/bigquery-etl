CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_glam_nightly__view_clients_daily_scalar_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix__view_clients_daily_scalar_aggregates_v1
  WHERE
    `moz-fx-data-shared-prod`.udf.fenix_build_to_datetime(app_build_id) >= date "2020-07-03"
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_nightly__view_clients_daily_scalar_aggregates_v1
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fennec_aurora__view_clients_daily_scalar_aggregates_v1
)
SELECT
  * EXCEPT (channel),
  "*" AS channel
FROM
  extracted
