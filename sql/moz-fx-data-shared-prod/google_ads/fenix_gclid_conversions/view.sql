CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.google_ads.fenix_gclid_conversions`
AS
SELECT
  gclid,
  conversion_event,
  FORMAT_DATETIME("%F %T", DATETIME(activity_date, TIME(23, 59, 59))) AS activity_date_timestamp,
FROM
  `moz-fx-data-shared-prod.google_ads_derived.fenix_gclid_conversions_v1`
WHERE
  report_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
