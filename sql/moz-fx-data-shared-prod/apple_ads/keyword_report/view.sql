CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.apple_ads.keyword_report`
AS
SELECT
  *
FROM
  `moz-fx-data-bq-fivetran.apple_ads_derived.keyword_report_v1`
