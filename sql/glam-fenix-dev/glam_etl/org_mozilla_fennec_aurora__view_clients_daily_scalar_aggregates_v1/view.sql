-- view for org_mozilla_fennec_aurora__view_clients_daily_scalar_aggregates_v1;
-- View to union daily scalar aggregates with date partitioning
CREATE OR REPLACE VIEW
  `glam-fenix-dev.glam_etl.org_mozilla_fennec_aurora__view_clients_daily_scalar_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `glam-fenix-dev.glam_etl.org_mozilla_fennec_aurora__clients_daily_scalar_aggregates*`
