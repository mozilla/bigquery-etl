WITH new_profiles AS (
  SELECT
    *,
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_activation AS (
  SELECT
    first_seen_date,
    client_id,
    is_activated,
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_activation_clients`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 21 DAY)
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_retention AS (
  SELECT
    first_seen_date,
    client_id,
    retained_week_4,
  FROM
    `moz-fx-data-shared-prod.fenix.retention_clients`
  WHERE
    submission_date = @submission_date
    AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  first_seen_date,
  new_profiles.normalized_channel,
  new_profiles.app_name,
  new_profiles.app_version,
  new_profiles.country,
  new_profiles.city,
  new_profiles.geo_subdivision,
  new_profiles.os,
  new_profiles.os_version,
  new_profiles.device_manufacturer,
  new_profiles.is_mobile,
  new_profiles.play_store_attribution_campaign,
  new_profiles.play_store_attribution_medium,
  new_profiles.play_store_attribution_source,
  new_profiles.play_store_attribution_content,
  new_profiles.play_store_attribution_term,
  new_profiles.meta_attribution_app,
  new_profiles.install_source,
  new_profiles.adjust_ad_group,
  new_profiles.adjust_campaign,
  new_profiles.adjust_creative,
  new_profiles.adjust_network,
  new_profiles.distribution_id,
  new_profiles.device_type,
  COUNT(new_profiles.client_id) AS new_profiles,
  COUNTIF(profile_activation.is_activated) AS activations,
  COUNTIF(profile_retention.retained_week_4) AS retained_week_4,
FROM
  new_profiles
LEFT JOIN
  profile_activation
  USING (first_seen_date, client_id)
LEFT JOIN
  profile_retention
  USING (first_seen_date, client_id)
GROUP BY
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  os,
  os_version,
  device_manufacturer,
  is_mobile,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term,
  meta_attribution_app,
  install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  distribution_id,
  device_type
