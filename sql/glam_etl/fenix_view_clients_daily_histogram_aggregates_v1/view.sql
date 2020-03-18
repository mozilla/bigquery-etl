-- Manually written view that handles time-partitioning
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.fenix_view_clients_daily_histogram_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `moz-fx-data-shared-prod.glam_etl.fenix_clients_daily_histogram_aggregates*`
