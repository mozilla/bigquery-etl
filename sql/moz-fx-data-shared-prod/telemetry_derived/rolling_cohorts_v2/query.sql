--Desktop
SELECT
  au.client_id,
  au.first_seen_date AS cohort_date,
  au.activity_segment,
  au.app_version,
  CAST(NULL AS string) AS attribution_campaign, --FIX
  CAST(NULL AS string) AS attribution_content, --FIX
  CAST(NULL AS string) AS attribution_experiment, --FIX
  au.attribution_medium,
  au.attribution_source,
  CAST(NULL AS string) AS attribution_variation, --FIX
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
FROM
  `moz-fx-data-shared-prod.telemetry.desktop_active_users` au
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen` ccls
  ON au.client_id = ccls.client_id
  AND au.submission_date = ccls.submission_date
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
  CAST(NULL AS BOOLEAN) AS is_default_browser,
  au.locale,
  CAST(NULL AS STRING) AS normalized_app_name,--FIX
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
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_active_users` au
WHERE
  au.first_seen_date = @submission_date
  AND au.submission_date = @submission_date
