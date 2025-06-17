WITH clients_first_seen AS (
  SELECT
    client_id,
    first_seen_date,
    is_desktop,
    attribution.medium AS attribution_medium,
    attribution.source AS attribution_source,
    attribution.campaign AS attribution_campaign,
    attribution.content AS attribution_content,
    attribution_dlsource,
    attribution_ua,
    (attribution.medium IS NOT NULL OR attribution.source IS NOT NULL) AS attributed,
    `moz-fx-data-shared-prod.udf.organic_vs_paid_desktop`(attribution.medium) AS paid_vs_organic,
    city,
    country,
    distribution_id,
    normalized_channel AS channel,
    normalized_os,
    normalized_os_version,
    locale,
    app_display_version,
    windows_version,
    windows_build_number
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_first_seen` cfs
  WHERE
    cfs.submission_date = @submission_date
),
active_users AS (
  SELECT
    client_id,
    is_dau,
    submission_date
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` au
  WHERE
    au.submission_date = @submission_date
)
SELECT
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  cfs.attributed,
  cfs.paid_vs_organic,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  cfs.channel,
  cfs.normalized_os,
  COALESCE(
    mozfun.norm.glean_windows_version_info(
      cfs.normalized_os,
      cfs.normalized_os_version,
      cfs.windows_build_number
    ),
    NULLIF(SPLIT(cfs.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ) AS normalized_os_version,
  cfs.app_display_version AS app_version,
  cfs.locale,
  cfs.windows_version,
  cfs.windows_build_number,
  COALESCE(au.is_dau, FALSE) AS is_dau,
  COUNT(cfs.client_id) AS new_profiles
FROM
  clients_first_seen cfs
LEFT JOIN
  active_users au
  ON cfs.client_id = au.client_id
  AND cfs.first_seen_date = au.submission_date
GROUP BY
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  cfs.attributed,
  cfs.paid_vs_organic,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  cfs.channel,
  cfs.normalized_os,
  COALESCE(
    mozfun.norm.glean_windows_version_info(
      cfs.normalized_os,
      cfs.normalized_os_version,
      cfs.windows_build_number
    ),
    NULLIF(SPLIT(cfs.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ),
  cfs.app_display_version,
  cfs.locale,
  cfs.windows_version,
  cfs.windows_build_number,
  au.is_dau
