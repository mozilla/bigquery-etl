-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  normalized_channel,
  NULLIF(adjust_info.adjust_ad_group, "") AS adjust_ad_group,
  NULLIF(adjust_info.adjust_campaign, "") AS adjust_campaign,
  NULLIF(adjust_info.adjust_creative, "") AS adjust_creative,
  NULLIF(adjust_info.adjust_network, "") AS adjust_network,
  adjust_info.adjust_attribution_timestamp,
  is_suspicious_device_client,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(
    adjust_info.adjust_network
  ) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.attribution_clients_v1`
