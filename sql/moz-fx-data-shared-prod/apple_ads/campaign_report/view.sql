CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.apple_ads.campaign_report`
AS
SELECT
  *
FROM
  `moz-fx-data-bq-fivetran.apple_ads_derived.campaign_report_v1`
