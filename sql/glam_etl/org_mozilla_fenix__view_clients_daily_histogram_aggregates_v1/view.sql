-- view for org_mozilla_fenix__view_clients_daily_histogram_aggregates_v1;
-- View for histogram aggregates that handles time-partitioning
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix__view_clients_daily_histogram_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix__clients_daily_histogram_aggregates*`
