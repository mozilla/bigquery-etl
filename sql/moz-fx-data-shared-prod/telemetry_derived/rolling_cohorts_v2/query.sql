--Since is_default_browser is not available on active_users, create a CTE with this information for mobile clients
WITH get_default_browser_for_mobile AS (
  (
    SELECT
      submission_date,
      client_id,
      is_default_browser,
      'Focus iOS' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.baseline_clients_last_seen_v1`
    WHERE
      submission_date = @submission_date
    UNION ALL
    SELECT
      submission_date,
      client_id,
      is_default_browser,
      'Fenix' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen`
    WHERE
      submission_date = @submission_date
    UNION ALL
    SELECT
      submission_date,
      client_id,
      is_default_browser,
      'Firefox iOS' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.firefox_ios.baseline_clients_last_seen`
    WHERE
      submission_date = @submission_date
    UNION ALL
    SELECT
      submission_date,
      client_id,
      is_default_browser,
      'Focus Android Glean' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.focus_android.baseline_clients_last_seen`
    WHERE
      submission_date = @submission_date
    UNION ALL
    SELECT
      submission_date,
      client_id,
      default_browser AS is_default_browser,
      'Focus Android' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
    WHERE
      submission_date = @submission_date
      AND app_name = 'Focus'
      AND os = 'Android'
    UNION ALL
    SELECT
      submission_date,
      client_id,
      is_default_browser,
      'Klar iOS' AS normalized_app_name
    FROM
      `moz-fx-data-shared-prod.klar_ios.clients_last_seen_joined`
    WHERE
      submission_date = @submission_date
  )
)
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
  au.os AS normalized_os, --old one had it as normalized_os, do I need to add a transform of some kind to normalize?
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
FROM
  `moz-fx-data-shared-prod.telemetry.desktop_active_users` au
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen` ccls
  ON au.client_id = ccls.client_id
  AND au.submission_date = ccls.submission_date
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.clients_last_seen` cls
  ON au.client_id = cls.client_id
  AND au.submission_date = cls.submission_date
WHERE
  au.first_seen_date = @submission_date
  AND au.submission_date = @submission_date
UNION ALL
--Mobile
SELECT
  au.client_id,
  au.first_seen_date AS cohort_date,
  au.activity_segment,
  CAST(NULL AS STRING) AS app_version, --FIX
  CAST(NULL AS string) AS attribution_campaign, --FIX
  CAST(NULL AS string) AS attribution_content, --FIX
  CAST(NULL AS string) AS attribution_experiment, --FIX
  CAST(NULL AS string) AS attribution_medium, --FIX
  CAST(NULL AS string) AS attribution_source, --FIX
  CAST(NULL AS string) AS attribution_variation, --FIX
  au.city,
  au.country,
  au.device_model,
  au.distribution_id,
  dflt_brwsr.is_default_browser,
  au.locale,
  au.app_name AS normalized_app_name, --do I need to do anything to "normalize" ?
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
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_active_users` au
LEFT OUTER JOIN
--need to check this below still, not correct yet
  get_default_browser_for_mobile dflt_brwsr
  ON au.client_id = dflt_brwsr.client_id
  AND au.submission_date = dflt_brwsr.submission_date
  AND au.app_name = dflt_brwsr.normalized_app_name --need to check this still
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.mobile_new_profile_clients` mnpc
  ON au.client_id = mnpc.client_id
  AND au.submission_date = mnpc.first_seen_date
WHERE
  au.first_seen_date = @submission_date
  AND au.submission_date = @submission_date
