SELECT
  cls.submission_date,
  cls.client_id,
  cls.sample_id,
  cls.distribution_id,
  cls.locale,
  cfs.first_seen_date,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_medium,
  cfs.attribution_ua,
  cd.normalized_channel,
  mozfun.norm.os(cd.os) AS normalized_os,
  COALESCE(
    mozfun.norm.windows_version_info(cd.os, cd.os_version, cd.windows_build_number),
    NULLIF(SPLIT(cd.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ) AS normalized_os_version,
  cd.country,
  cls.is_dau AS dau,
  cls.is_wau AS wau,
  cls.is_mau AS mau
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2` cls
LEFT JOIN
  `mozdata.telemetry.clients_first_seen` cfs
  ON cls.client_id = cfs.client_id
LEFT JOIN
  `mozdata.telemetry.clients_daily_v6` cd
  ON cls.client_id = cd.client_id
  AND cls.submission_date = cd.submission_date
  AND cd.submission_date = @submission_date
WHERE
  cls.submission_date = @submission_date
