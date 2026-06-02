CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.statcounter.browser_market_share_regions`
AS
SELECT
  `date`,
  `geography`,
  device,
  browser,
  `percent`
FROM
  `moz-fx-data-shared-prod.statcounter_derived.browser_market_share_regions_v1`
