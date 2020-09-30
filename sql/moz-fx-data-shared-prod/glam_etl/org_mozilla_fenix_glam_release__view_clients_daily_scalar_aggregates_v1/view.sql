CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_glam_release__view_clients_daily_scalar_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_firefox__view_clients_daily_scalar_aggregates_v1
)
SELECT
  * EXCEPT (channel),
  "*" AS channel
FROM
  extracted
