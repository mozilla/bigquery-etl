SELECT
  au.submission_date,
  au.client_id,
  au.sample_id,
  cfs.first_seen_date,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_medium,
  cfs.attribution_ua,
  mozfun.norm.os(cd.os) AS normalized_os,
  COALESCE(
    mozfun.norm.windows_version_info(cd.os, cd.os_version, cd.windows_build_number),
    NULLIF(SPLIT(cd.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ) AS normalized_os_version,
  cd.country,
  au.is_dau AS dau,
  au.is_wau AS wau,
  au.is_mau AS mau
FROM
  `moz-fx-data-shared-prod.telemetry.active_users` au
LEFT JOIN
  `mozdata.telemetry.clients_first_seen` cfs
  ON au.client_id = cfs.client_id
LEFT JOIN
  `mozdata.telemetry.clients_daily_v6` cd
  ON au.client_id = cd.client_id
  AND au.submission_date = cd.submission_date
  AND cd.submission_date = @submission_date
WHERE
  au.app_name = 'Firefox'
  AND au.submission_date = @submission_date
