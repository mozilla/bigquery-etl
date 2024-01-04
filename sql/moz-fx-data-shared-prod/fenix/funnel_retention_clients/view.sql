CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.funnel_retention_clients`
AS
SELECT
  first_seen_date,
  client_id,
  sample_id,
  COALESCE(
    retention_week_4.first_reported_country,
    retention_week_2.first_reported_country
  ) AS first_reported_country,
  COALESCE(
    retention_week_4.first_reported_isp,
    retention_week_2.first_reported_isp
  ) AS first_reported_isp,
  COALESCE(retention_week_4.adjust_ad_group, retention_week_2.adjust_ad_group) AS adjust_ad_group,
  COALESCE(retention_week_4.adjust_campaign, retention_week_2.adjust_campaign) AS adjust_campaign,
  COALESCE(retention_week_4.adjust_creative, retention_week_2.adjust_creative) AS adjust_creative,
  COALESCE(retention_week_4.adjust_network, retention_week_2.adjust_network) AS adjust_network,
  COALESCE(retention_week_4.install_source, retention_week_2.install_source) AS install_source,
  retention_week_2.retained_week_2,
  retention_week_4.retained_week_4,
  retention_week_4.days_seen_in_first_28_days,
  retention_week_4.repeat_first_month_user,
FROM
  `moz-fx-data-shared-prod.fenix_derived.funnel_retention_clients_week_2_v1` AS retention_week_2
FULL OUTER JOIN
  `moz-fx-data-shared-prod.fenix_derived.funnel_retention_clients_week_4_v1` AS retention_week_4
USING
  (first_seen_date, client_id, sample_id)
