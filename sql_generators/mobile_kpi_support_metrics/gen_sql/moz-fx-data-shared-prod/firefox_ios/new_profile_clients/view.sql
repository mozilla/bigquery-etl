-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profile_clients`
AS
SELECT
  client_id,
  first_seen_date,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  locale,
  isp,
  is_mobile,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  attribution.is_suspicious_device_client,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users`
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients` AS attribution
  USING (client_id)
WHERE
  submission_date < CURRENT_DATE
  AND is_new_profile
