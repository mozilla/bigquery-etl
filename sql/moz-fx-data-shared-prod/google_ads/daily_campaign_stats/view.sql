CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.google_ads.daily_campaign_stats`
AS
SELECT
  * REPLACE (REGEXP_REPLACE(campaign_name, '(.*) - NULL', '\\1') AS campaign_name)
FROM
  `moz-fx-data-shared-prod.google_ads_derived.daily_campaign_stats_v1`
