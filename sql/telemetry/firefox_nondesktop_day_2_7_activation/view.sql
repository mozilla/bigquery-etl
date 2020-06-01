CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_day_2_7_activation`
AS
SELECT
  DATE_SUB(submission_date, INTERVAL 6 DAY) AS cohort_date,
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_nondesktop_day_2_7_activation_v1`
