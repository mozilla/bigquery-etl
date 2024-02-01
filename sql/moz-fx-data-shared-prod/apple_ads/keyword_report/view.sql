CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.apple_ads.keyword_report`
AS
-- As per the Fivetran connector docs (https://fivetran.com/docs/applications/apple-search-ads#utcconversion):
-- "We don't convert source timestamps to Universal Time Coordinated (UTC)
-- but use the Apple Search Ads account's time zone to store the data in your destination."
--
-- ! We should be careful about this and make sure our account is set to UTC and not changed.
SELECT
  *
FROM
  `moz-fx-data-shared-prod.apple_ads_external.keyword_report_v1`
