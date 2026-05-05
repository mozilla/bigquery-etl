--Desktop
SELECT
  au.client_id,
  au.first_seen_date AS cohort_date,
  au.activity_segment,
  au.app_version,
  cls.attribution.campaign AS attribution_campaign,
  cls.attribution.content AS attribution_content,
  cls.attribution.experiment AS attribution_experiment,
  au.attribution_medium,
  au.attribution_source,
  cls.attribution.variation AS attribution_variation,
  au.city,
  au.country,
  ccls.device AS device_model,
  au.distribution_id,
  au.is_default_browser,
  au.locale,
  au.app_name AS normalized_app_name,
  au.normalized_channel,
  au.os AS normalized_os,
  au.normalized_os_version,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(au.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
    0
  ) AS os_version_major,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(au.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
    0
  ) AS os_version_minor,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_content,
  CAST(NULL AS STRING) AS play_store_attribution_term,
  'Desktop' AS row_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
FROM
  `moz-fx-data-shared-prod.telemetry.desktop_active_users` au
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen` ccls
  USING (client_id, submission_date)
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.clients_last_seen` cls
  USING (client_id, submission_date)
WHERE
  au.first_seen_date = @submission_date
  AND au.submission_date = @submission_date
UNION ALL
--Mobile
SELECT
  au.client_id,
  au.first_seen_date AS cohort_date,
  au.activity_segment,
  au.app_display_version AS app_version,
  CAST(NULL AS STRING) AS attribution_campaign,
  CAST(NULL AS STRING) AS attribution_content,
  CAST(NULL AS STRING) AS attribution_experiment,
  CAST(NULL AS STRING) AS attribution_medium,
  CAST(NULL AS STRING) AS attribution_source,
  CAST(NULL AS STRING) AS attribution_variation,
  au.city,
  au.country,
  au.device_model,
  au.distribution_id,
  CAST(NULL AS BOOLEAN) AS is_default_browser,
  au.locale,
  au.app_name AS normalized_app_name,
  au.normalized_channel,
  au.normalized_os,
  au.normalized_os_version,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(au.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
    0
  ) AS os_version_major,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(au.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
    0
  ) AS os_version_minor,
  mnpc.adjust_ad_group,
  mnpc.adjust_campaign,
  mnpc.adjust_creative,
  mnpc.adjust_network,
  mnpc.play_store_attribution_campaign,
  mnpc.play_store_attribution_medium,
  mnpc.play_store_attribution_source,
  mnpc.play_store_attribution_content,
  mnpc.play_store_attribution_term,
  'Mobile' AS row_source,
  mnpc.play_store_attribution_install_referrer_response
FROM
  (
--in case of multiple rows per client ID / first seen date (rare), pick 1
    SELECT
      client_id,
      submission_date,
      first_seen_date,
      activity_segment,
      app_display_version,
      city,
      country,
      device_model,
      distribution_id,
      locale,
      app_name,
      normalized_channel,
      normalized_os,
      normalized_os_version
    FROM
      `moz-fx-data-shared-prod.telemetry.mobile_active_users`
    WHERE
      first_seen_date = @submission_date
      AND submission_date = @submission_date
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id) = 1
  ) au
LEFT JOIN
  (
--in case of multiple rows per client ID /first seen date (rare), pick 1
    SELECT
      first_seen_date,
      client_id,
      adjust_ad_group,
      adjust_campaign,
      adjust_creative,
      adjust_network,
      play_store_attribution_campaign,
      play_store_attribution_medium,
      play_store_attribution_source,
      play_store_attribution_content,
      play_store_attribution_term,
      play_store_attribution_install_referrer_response
    FROM
      `moz-fx-data-shared-prod.telemetry.mobile_new_profile_clients`
    WHERE
      first_seen_date = @submission_date
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id) = 1
  ) mnpc
  ON au.client_id = mnpc.client_id
