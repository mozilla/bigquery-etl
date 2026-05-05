-- view for firefox_desktop__view_clients_daily_histogram_aggregates_v1;
-- View for histogram aggregates that handles time-partitioning
CREATE OR REPLACE VIEW
  `glam-fenix-dev.glam_etl.firefox_desktop__view_clients_daily_histogram_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `glam-fenix-dev.glam_etl.firefox_desktop__clients_daily_histogram_aggregates*`
