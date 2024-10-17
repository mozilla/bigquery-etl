-- Query generated via `mobile_kpi_support_metrics` SQL generator.
SELECT
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  os,
  os_version,
  device_manufacturer,
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
  COUNT(*) AS new_profiles,
FROM
  `moz-fx-data-shared-prod.fenix.new_profile_clients`
WHERE
  {% if is_init() %}
    first_seen_date < CURRENT_DATE
  {% else %}
    first_seen_date = @submission_date
  {% endif %}
GROUP BY
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  os,
  os_version,
  device_manufacturer,
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
