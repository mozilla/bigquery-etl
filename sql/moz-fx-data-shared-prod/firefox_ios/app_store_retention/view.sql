CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.app_store_retention`
AS
SELECT
  first_seen_date,
  client_id,
  sample_id,
  retention_week_2.retained_week_2,
  retention_week_4.retained_week_4,
  retention_week_4.days_seen_in_first_28_days,
  retention_week_4.repeat_first_month_user,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_retention_week_2_v1` AS retention_week_2
FULL OUTER JOIN
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_retention_week_4_v1` AS retention_week_4
USING
  (first_seen_date, client_id, sample_id)
