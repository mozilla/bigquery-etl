-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.attribution_clients`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  normalized_channel,
  NULLIF(play_store_info.play_store_attribution_campaign, "") AS play_store_attribution_campaign,
  NULLIF(play_store_info.play_store_attribution_medium, "") AS play_store_attribution_medium,
  NULLIF(play_store_info.play_store_attribution_source, "") AS play_store_attribution_source,
  play_store_info.play_store_attribution_timestamp,
  NULLIF(play_store_info.play_store_attribution_content, "") AS play_store_attribution_content,
  NULLIF(play_store_info.play_store_attribution_term, "") AS play_store_attribution_term,
  NULLIF(
    play_store_info.play_store_attribution_install_referrer_response,
    ""
  ) AS play_store_attribution_install_referrer_response,
  NULLIF(meta_info.meta_attribution_app, "") AS meta_attribution_app,
  meta_info.meta_attribution_timestamp,
  NULLIF(adjust_info.adjust_ad_group, "") AS adjust_ad_group,
  CASE
    WHEN adjust_info.adjust_network IN ('Google Organic Search', 'Organic')
      THEN 'Organic'
    ELSE NULLIF(adjust_info.adjust_campaign, "")
  END AS adjust_campaign,
  NULLIF(adjust_info.adjust_creative, "") AS adjust_creative,
  NULLIF(adjust_info.adjust_network, "") AS adjust_network,
  adjust_info.adjust_attribution_timestamp,
  install_source,
  distribution_id,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(
    adjust_info.adjust_network
  ) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix_derived.attribution_clients_v1`
