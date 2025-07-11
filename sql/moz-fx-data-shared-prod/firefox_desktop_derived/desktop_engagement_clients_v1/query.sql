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
  aud.is_mau,
  COALESCE(
    cls.legacy_telemetry_client_id,
    cfs.legacy_telemetry_client_id
  ) AS legacy_telemetry_client_id,
  mozfun.norm.glean_windows_version_info(
    cls.normalized_os,
    cls.normalized_os_version,
    cls.windows_build_number
  ) AS windows_version
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` cls
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen` cfs
  ON cls.client_id = cfs.client_id
  AND cfs.submission_date <= @submission_date
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` aud
  ON cls.client_id = aud.client_id
  AND cls.submission_date = aud.submission_date
LEFT JOIN
  (
    SELECT DISTINCT
      client_info.client_id AS client_id
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.deletion_request_v1`
    WHERE
      DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 17 WEEK)
  ) AS deletion_requests
  ON cls.client_id = deletion_requests.client_id
WHERE
  cls.submission_date = @submission_date
  AND deletion_requests.client_id IS NULL
