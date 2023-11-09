SELECT
  first_seen_date,
  submission_date,
  -- activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  -- attribution_variation,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  city,
  country,
  -- device_model,
  apple_model_id, -- from clients_first_seen_v2
  distribution_id,
  -- is_default_browser,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
  COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
  COUNT(client_id) AS num_clients_in_cohort,
  COUNTIF((active_client_id IS NOT NULL) AND (active_clients_days_since_seen = 0)) AS num_clients_active_on_day,
  COUNTIF((active_client_id IS NOT NULL) AND
    (COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & active_client_days_seen_bits) > 0,
    FALSE
    )) ) AS num_clients_active_atleastonce_in_last_7_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    ( COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & active_client_days_seen_bits) > 0,
    FALSE) )) AS num_clients_active_atleastonce_in_last_28_days,
FROM
  clients_daily_statistics
GROUP BY
  first_seen_date,
  submission_date,
  -- activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  -- attribution_variation,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  city,
  country,
  -- device_model,
  apple_model_id,
  distribution_id,
  -- is_default_browser,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor
