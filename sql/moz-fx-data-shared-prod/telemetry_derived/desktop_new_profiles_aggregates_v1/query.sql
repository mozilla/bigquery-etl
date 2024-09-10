SELECT
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  (cfs.attribution_medium IS NOT NULL OR cfs.attribution_source IS NOT NULL) AS attributed,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  EXTRACT(YEAR FROM cfs.first_seen_date) AS first_seen_year,
  cfs.normalized_channel AS channel,
  cfs.os,
  cfs.os_version,
  cfs.normalized_os,
  cfs.normalized_os_version,
  cfs.locale,
  cfs.app_version,
  cfs.windows_version,
  cfs.windows_build_number,
  au.is_dau,
  COUNT(cfs.client_id) AS new_profiles
FROM
  `moz-fx-data-shared-prod.telemetry.clients_first_seen` cfs
INNER JOIN
  `moz-fx-data-shared-prod.telemetry.active_users` au
  ON au.client_id = cfs.client_id
WHERE
  cfs.first_seen_date = @submission_date
  AND au.submission_date = @submission_date
GROUP BY
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  attributed,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  first_seen_year,
  channel,
  cfs.os,
  cfs.os_version,
  normalized_os,
  normalized_os_version,
  cfs.locale,
  cfs.app_version,
  cfs.windows_version,
  cfs.windows_build_number,
  au.is_dau
