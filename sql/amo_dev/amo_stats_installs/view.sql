CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.amo_dev.amo_stats_installs`
AS
SELECT
  DATE_SUB(submission_date, INTERVAL 2 DAY) AS install_date,
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-shared-prod.amo_dev.amo_stats_installs_v1`
