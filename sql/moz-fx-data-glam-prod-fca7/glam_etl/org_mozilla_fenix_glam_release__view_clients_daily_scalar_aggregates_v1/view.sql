CREATE OR REPLACE VIEW
  `moz-fx-data-glam-prod-fca7`.glam_etl.org_mozilla_fenix_glam_release__view_clients_daily_scalar_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-glam-prod-fca7`.glam_etl.org_mozilla_firefox__view_clients_daily_scalar_aggregates_v1
)
SELECT
  * EXCEPT (app_build_id, channel),
  mozfun.glam.fenix_build_to_build_hour(app_build_id) AS app_build_id,
  "*" AS channel
FROM
  extracted
