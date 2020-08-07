CREATE OR REPLACE VIEW
  glam_etl.org_mozilla_fenix_glam_release__view_clients_daily_scalar_aggregates_v1
AS
WITH unioned AS (
  SELECT
    *
  FROM
    glam_etl.org_mozilla_firefox__view_clients_daily_scalar_aggregates_v1
)
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  unioned
