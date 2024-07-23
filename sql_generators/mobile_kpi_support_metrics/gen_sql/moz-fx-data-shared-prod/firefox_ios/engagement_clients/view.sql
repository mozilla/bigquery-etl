-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.engagement_clients`
AS
WITH active_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    first_seen_date,
    app_name,
    normalized_channel,
    locale,
    country,
    isp,
    app_display_version,
    is_dau,
    is_wau,
    is_mau,
    is_mobile,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.active_users`
)
SELECT
  submission_date,
  client_id,
  sample_id,
  first_seen_date,
  app_name,
  normalized_channel,
  app_display_version AS app_version,
  locale,
  country,
  isp,
  is_dau,
  is_wau,
  is_mau,
  is_mobile,
  NULLIF(attribution.adjust_ad_group, "") AS adjust_ad_group,
  NULLIF(attribution.adjust_campaign, "") AS adjust_campaign,
  NULLIF(attribution.adjust_creative, "") AS adjust_creative,
  NULLIF(attribution.adjust_network, "") AS adjust_network,
  attribution.is_suspicious_device_client,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(
    attribution.adjust_network
  ) AS paid_vs_organic,
  CASE
    WHEN active_users.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
FROM
  active_users
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients` AS attribution
  USING (client_id)
