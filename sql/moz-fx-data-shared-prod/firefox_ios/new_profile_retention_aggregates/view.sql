CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profile_retention_aggregates`
AS
SELECT
  `date`,
  first_seen_date,
  channel,
  country,
  isp,
  -- adjust_ad_group,
  -- adjust_campaign,
  -- adjust_creative,
  -- adjust_network,
  COUNT(*) AS new_profiles,
  COUNTIF(repeat_first_month_user) AS repeat_user,
  COUNTIF(retained_week_4) AS retained_week_4,
FROM
  firefox_ios.new_profile_retention
GROUP BY
  `date`,
  first_seen_date,
  channel,
  country,
  isp,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network
