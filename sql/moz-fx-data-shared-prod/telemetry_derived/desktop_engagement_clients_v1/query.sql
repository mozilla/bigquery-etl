SELECT
  cls.submission_date,
  cls.client_id,
  cls.sample_id,
  cls.distribution_id,
  cls.locale,
  cls.app_version,
  cls.isp_name AS isp,
  cfs.first_seen_date,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_medium,
  cfs.attribution_ua,
  cfs.attribution_experiment,
  cls.attribution.variation AS attribution_variation,
  cls.normalized_channel,
  mozfun.norm.os(cls.os) AS normalized_os,
  COALESCE(
    mozfun.norm.windows_version_info(cls.os, cls.os_version, cls.windows_build_number),
    NULLIF(SPLIT(cls.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
  ) AS normalized_os_version,
  cls.startup_profile_selection_reason_first AS startup_profile_selection_reason,
  cls.country,
  cls.is_dau,
  cls.is_wau,
  cls.is_mau
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2` cls
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.clients_first_seen` cfs
  ON cls.client_id = cfs.client_id
WHERE
  cls.submission_date = @submission_date
