CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
AS
SELECT
  -- https://developer.apple.com/help/app-store-connect/view-sales-and-trends/download-and-view-reports
  -- "Time zone: Reports are based on Pacific Time (PT). A day includes transactions that happened from 12:00 a.m. to 11:59 p.m. PT."
  -- Date conversion in the query is required to unify the dates to UTC timezone which is what we use.
  * REPLACE (TIMESTAMP(`date`, "America/Los_Angeles") AS `date`),
  `date` AS date_pst,
FROM
  `moz-fx-data-shared-prod.app_store_external.firefox_app_store_territory_source_type_report_v1`
