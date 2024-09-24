-- Query generated via `mobile_kpi_support_metrics` SQL generator.
SELECT
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  meta_attribution_app,
  install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  distribution_id,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
FROM
  `moz-fx-data-shared-prod.fenix.engagement_clients`
WHERE
  {% if is_init() %}
    submission_date < CURRENT_DATE
  {% else %}
    submission_date = @submission_date
  {% endif %}
GROUP BY
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  meta_attribution_app,
  install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  distribution_id
