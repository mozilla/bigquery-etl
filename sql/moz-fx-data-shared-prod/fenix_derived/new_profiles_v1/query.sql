-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH device_manufacturer_counts AS (
  SELECT
    first_seen_date,
    device_manufacturer,
    RANK() OVER (PARTITION BY first_seen_date ORDER BY COUNT(*) DESC) AS manufacturer_rank,
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
    device_manufacturer
)
SELECT
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  os,
  os_version,
  -- Bucket device manufacturers with low count prior to aggregation
  IF(manufacturer_rank <= 150, device_manufacturer, "other") AS device_manufacturer,
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
  device_type,
FROM
  `moz-fx-data-shared-prod.fenix.new_profile_clients`
LEFT JOIN
  device_manufacturer_counts
  USING (first_seen_date, device_manufacturer)
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
  device_type,
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
