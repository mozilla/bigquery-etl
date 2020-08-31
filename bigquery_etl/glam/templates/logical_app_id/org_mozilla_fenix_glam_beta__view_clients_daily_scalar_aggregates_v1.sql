CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_glam_beta__view_clients_daily_scalar_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix__view_clients_daily_scalar_aggregates_v1
  WHERE
    mozfun.norm.fenix_app_info('org_mozilla_fenix', app_build_id).channel = 'beta'
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_firefox_beta__view_clients_daily_scalar_aggregates_v1
)
SELECT
  * EXCEPT (channel),
  "*" AS channel
FROM
  extracted
