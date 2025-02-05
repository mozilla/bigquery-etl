WITH new_profiles AS (
  SELECT
    * REPLACE (
      IFNULL(app_version, "null") AS app_version,
      IFNULL(country, "null") AS country,
      IFNULL(city, "null") AS city,
      IFNULL(geo_subdivision, "null") AS geo_subdivision,
      IFNULL(locale, "null") AS locale,
        -- IFNULL(isp, "null") AS isp,
      IFNULL(os, "null") AS os,
      IFNULL(os_version, "null") AS os_version,
      IFNULL(device_manufacturer, "null") AS device_manufacturer,
      COALESCE(is_mobile, FALSE) AS is_mobile,
      IFNULL(play_store_attribution_campaign, "null") AS play_store_attribution_campaign,
      IFNULL(play_store_attribution_medium, "null") AS play_store_attribution_medium,
      IFNULL(play_store_attribution_source, "null") AS play_store_attribution_source,
      IFNULL(play_store_attribution_content, "null") AS play_store_attribution_content,
      IFNULL(play_store_attribution_term, "null") AS play_store_attribution_term,
      IFNULL(meta_attribution_app, "null") AS meta_attribution_app,
      IFNULL(install_source, "null") AS install_source,
      IFNULL(adjust_ad_group, "null") AS adjust_ad_group,
      IFNULL(adjust_campaign, "null") AS adjust_campaign,
      IFNULL(adjust_creative, "null") AS adjust_creative,
      IFNULL(adjust_network, "null") AS adjust_network,
      IFNULL(distribution_id, "null") AS distribution_id,
      IFNULL(device_type, "null") AS device_type
    )
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_activation AS (
  SELECT
    * EXCEPT (submission_date) REPLACE(
      IFNULL(app_version, "null") AS app_version,
      IFNULL(country, "null") AS country,
      IFNULL(city, "null") AS city,
      IFNULL(geo_subdivision, "null") AS geo_subdivision,
      IFNULL(locale, "null") AS locale,
        -- IFNULL(isp, "null") AS isp,
      IFNULL(os, "null") AS os,
      IFNULL(os_version, "null") AS os_version,
      IFNULL(device_manufacturer, "null") AS device_manufacturer,
      COALESCE(is_mobile, FALSE) AS is_mobile,
      IFNULL(play_store_attribution_campaign, "null") AS play_store_attribution_campaign,
      IFNULL(play_store_attribution_medium, "null") AS play_store_attribution_medium,
      IFNULL(play_store_attribution_source, "null") AS play_store_attribution_source,
      IFNULL(play_store_attribution_content, "null") AS play_store_attribution_content,
      IFNULL(play_store_attribution_term, "null") AS play_store_attribution_term,
      IFNULL(meta_attribution_app, "null") AS meta_attribution_app,
      IFNULL(install_source, "null") AS install_source,
      IFNULL(adjust_ad_group, "null") AS adjust_ad_group,
      IFNULL(adjust_campaign, "null") AS adjust_campaign,
      IFNULL(adjust_creative, "null") AS adjust_creative,
      IFNULL(adjust_network, "null") AS adjust_network,
      IFNULL(distribution_id, "null") AS distribution_id,
      IFNULL(device_type, "null") AS device_type
    ),
  FROM
    `moz-fx-data-shared-prod.fenix.new_profile_activation_clients`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 21 DAY)
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_retention AS (
  SELECT
    * EXCEPT (submission_date) REPLACE(
      IFNULL(app_version, "null") AS app_version,
      IFNULL(country, "null") AS country,
      IFNULL(city, "null") AS city,
      IFNULL(geo_subdivision, "null") AS geo_subdivision,
      IFNULL(locale, "null") AS locale,
        -- IFNULL(isp, "null") AS isp,
      IFNULL(os, "null") AS os,
      IFNULL(os_version, "null") AS os_version,
      IFNULL(device_manufacturer, "null") AS device_manufacturer,
      COALESCE(is_mobile, FALSE) AS is_mobile,
      IFNULL(play_store_attribution_campaign, "null") AS play_store_attribution_campaign,
      IFNULL(play_store_attribution_medium, "null") AS play_store_attribution_medium,
      IFNULL(play_store_attribution_source, "null") AS play_store_attribution_source,
      IFNULL(play_store_attribution_content, "null") AS play_store_attribution_content,
      IFNULL(play_store_attribution_term, "null") AS play_store_attribution_term,
      IFNULL(meta_attribution_app, "null") AS meta_attribution_app,
      IFNULL(install_source, "null") AS install_source,
      IFNULL(adjust_ad_group, "null") AS adjust_ad_group,
      IFNULL(adjust_campaign, "null") AS adjust_campaign,
      IFNULL(adjust_creative, "null") AS adjust_creative,
      IFNULL(adjust_network, "null") AS adjust_network,
      IFNULL(distribution_id, "null") AS distribution_id,
      IFNULL(device_type, "null") AS device_type
    ),
  FROM
    `moz-fx-data-shared-prod.fenix.retention_clients`
  WHERE
    submission_date = @submission_date
    AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
profile_engagement AS (
  SELECT
    * EXCEPT (submission_date) REPLACE(
      IFNULL(app_version, "null") AS app_version,
      IFNULL(country, "null") AS country,
      IFNULL(city, "null") AS city,
      IFNULL(geo_subdivision, "null") AS geo_subdivision,
      IFNULL(locale, "null") AS locale,
        -- IFNULL(isp, "null") AS isp,
      IFNULL(os, "null") AS os,
      IFNULL(os_version, "null") AS os_version,
      IFNULL(device_manufacturer, "null") AS device_manufacturer,
      COALESCE(is_mobile, FALSE) AS is_mobile,
      IFNULL(play_store_attribution_campaign, "null") AS play_store_attribution_campaign,
      IFNULL(play_store_attribution_medium, "null") AS play_store_attribution_medium,
      IFNULL(play_store_attribution_source, "null") AS play_store_attribution_source,
      IFNULL(play_store_attribution_content, "null") AS play_store_attribution_content,
      IFNULL(play_store_attribution_term, "null") AS play_store_attribution_term,
      IFNULL(meta_attribution_app, "null") AS meta_attribution_app,
      IFNULL(install_source, "null") AS install_source,
      IFNULL(adjust_ad_group, "null") AS adjust_ad_group,
      IFNULL(adjust_campaign, "null") AS adjust_campaign,
      IFNULL(adjust_creative, "null") AS adjust_creative,
      IFNULL(adjust_network, "null") AS adjust_network,
      IFNULL(distribution_id, "null") AS distribution_id,
      IFNULL(device_type, "null") AS device_type
    ),
  FROM
    `moz-fx-data-shared-prod.fenix.engagement_clients`
  WHERE
    submission_date = @submission_date
    AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  @submission_date AS submission_date,
  first_seen_date,
  NULLIF(normalized_channel, "null") AS normalized_channel,
  NULLIF(app_name, "null") AS app_name,
  NULLIF(app_version, "null") AS app_version,
  NULLIF(country, "null") AS country,
  NULLIF(city, "null") AS city,
  NULLIF(geo_subdivision, "null") AS geo_subdivision,
  NULLIF(locale, "null") AS locale,
  -- NULLIF(isp, "null") AS isp,
  NULLIF(os, "null") AS os,
  NULLIF(os_version, "null") AS os_version,
  NULLIF(device_manufacturer, "null") AS device_manufacturer,
  is_mobile AS is_mobile,
  NULLIF(play_store_attribution_campaign, "null") AS play_store_attribution_campaign,
  NULLIF(play_store_attribution_medium, "null") AS play_store_attribution_medium,
  NULLIF(play_store_attribution_source, "null") AS play_store_attribution_source,
  NULLIF(play_store_attribution_content, "null") AS play_store_attribution_content,
  NULLIF(play_store_attribution_term, "null") AS play_store_attribution_term,
  NULLIF(meta_attribution_app, "null") AS meta_attribution_app,
  NULLIF(install_source, "null") AS install_source,
  NULLIF(adjust_ad_group, "null") AS adjust_ad_group,
  NULLIF(adjust_campaign, "null") AS adjust_campaign,
  NULLIF(adjust_creative, "null") AS adjust_creative,
  NULLIF(adjust_network, "null") AS adjust_network,
  NULLIF(distribution_id, "null") AS distribution_id,
  NULLIF(device_type, "null") AS device_type,
  COUNT(new_profiles.client_id) AS new_profiles,
  COUNTIF(is_activated) AS activations,
  COUNTIF(is_early_engagement) AS early_engagements,
  COUNTIF(retained_week_4) AS retained_week_4,
  COUNTIF(repeat_profile) AS repeat_profile,
  COUNTIF(is_dau) AS dau,
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
    -- isp,
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
    -- isp,
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
    -- isp,
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
  )
GROUP BY
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  locale,
  -- isp,
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
