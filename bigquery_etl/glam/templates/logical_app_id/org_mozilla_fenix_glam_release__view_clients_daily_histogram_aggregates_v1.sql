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
  * EXCEPT (app_build_id, channel),
  mozfun.norm.fenix_build_to_build_hour(app_build_id) AS app_build_id,
  "*" AS channel
FROM
  extracted
