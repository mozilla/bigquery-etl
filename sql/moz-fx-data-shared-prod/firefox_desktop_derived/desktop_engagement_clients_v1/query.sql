SELECT
  cls.submission_date,
  cls.client_id,
  cls.profile_group_id,
  cls.sample_id,
  cls.distribution_id,
  cls.locale,
  cls.app_display_version AS app_version,
  cls.isp,
  cfs.first_seen_date,
  cfs.attribution.campaign AS attribution_campaign,
  cfs.attribution.content AS attribution_content,
  cfs.attribution_dlsource AS attribution_dlsource,
  cfs.attribution.medium AS attribution_medium,
  cfs.attribution_ua,
  cfs.attribution_experiment,
  cls.attribution_variation,
  cls.normalized_channel,
  cls.normalized_os,
  COALESCE(
    mozfun.norm.glean_windows_version_info(
      cls.normalized_os,
      cls.normalized_os_version,
      cls.windows_build_number
    ),
    NULLIF(SPLIT(cls.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ) AS normalized_os_version,
  cls.country,
  aud.is_desktop,
  aud.is_dau,
  aud.is_wau,
  aud.is_mau
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` cls
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_first_seen` cfs
  ON cls.client_id = cfs.client_id
  AND cfs.submission_date <= @submission_date
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` aud
  ON cls.client_id = aud.client_id
  AND cls.submission_date = aud.submission_date
WHERE
  cls.submission_date = @submission_date
