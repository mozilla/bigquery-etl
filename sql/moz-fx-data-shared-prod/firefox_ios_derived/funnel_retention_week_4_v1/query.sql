SELECT
  submission_date,
  first_seen_date,
  first_reported_country,
  first_reported_isp,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  COUNT(*) AS new_profiles,
  COUNTIF(repeat_first_month_user) AS repeat_user,
  COUNTIF(retained_week_4) AS retained_week_4,
FROM
  firefox_ios_derived.funnel_retention_clients_week_4_v1
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  first_seen_date,
  first_reported_country,
  first_reported_isp,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network
