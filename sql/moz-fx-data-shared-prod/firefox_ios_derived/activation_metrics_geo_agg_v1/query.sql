WITH new_profiles AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_ios.new_profile_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_activation AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_ios.new_profile_activation_clients`
  WHERE
    submission_date = @submission_date
),
profile_retention AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_ios.retention_clients`
  WHERE
    submission_date = @submission_date
    AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_engagement AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_ios.engagement_clients`
  WHERE
    submission_date = @submission_date
)
SELECT
  first_seen_date,
  client_id,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  os,
  os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  adjust_attribution_timestamp,
  device_type
  -- COUNTIF(is_dau) AS dau,
  -- COUNTIF(is_wau) AS wau,
  -- COUNTIF(is_mau) AS mau,
FROM
  new_profiles
LEFT JOIN
  profile_activation
  USING (
    first_seen_date,
    client_id,
    normalized_channel,
    app_name,
    app_version,
    country,
    city,
    geo_subdivision,
    locale,
    isp,
    os,
    os_version,
    device_model,
    device_manufacturer,
    is_mobile,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp,
    device_type
  )
LEFT JOIN
  profile_retention
  USING (
    first_seen_date,
    client_id,
    normalized_channel,
    app_name,
    app_version,
    country,
    city,
    geo_subdivision,
    locale,
    isp,
    os,
    os_version,
    device_model,
    device_manufacturer,
    is_mobile,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp,
    device_type
  )
LEFT JOIN
  profile_engagement
  USING (
    first_seen_date,
    client_id,
    normalized_channel,
    app_name,
    app_version,
    country,
    city,
    geo_subdivision,
    locale,
    isp,
    os,
    os_version,
    device_model,
    device_manufacturer,
    is_mobile,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp,
    device_type
  )
GROUP BY
  first_seen_date,
  client_id,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  os,
  os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  adjust_attribution_timestamp,
  device_type
