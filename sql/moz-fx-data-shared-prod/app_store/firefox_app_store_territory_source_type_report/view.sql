CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
AS
SELECT
  -- https://developer.apple.com/help/app-store-connect/view-sales-and-trends/download-and-view-reports
  -- "Time zone: Reports are based on Pacific Time (PT). A day includes transactions that happened from 12:00 a.m. to 11:59 p.m. PT."
  -- Date conversion in the query is required to unify the dates to UTC timezone which is what we use.
  -- However, the `date` timestamp field appear to always show midnight meaning if we do timezone conversion
  -- we will end up moving all results 1 day back if we attempt conversion to UTC.
  -- This is why we are not doing timezone converstions here.
  *,
FROM
  `moz-fx-data-shared-prod.app_store_syndicate.app_store_territory_source_type_report`
