SELECT
  client_id,
  normalized_os AS os,
  normalized_channel AS channel,
  SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
  application.build_id AS app_build_id,
  SUM(DISTINCT(IF(prefs.value = 'true', 2, 1))) AS has_fission,  -- 1 is non-fission, 2 is fission, 3 is mixed
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v4`
CROSS JOIN
  UNNEST(environment.settings.user_prefs) prefs
WHERE
  DATE(submission_timestamp) = @submission_date
  AND prefs.key = 'fission.autostart'
GROUP BY
  client_id,
  normalized_os,
  normalized_channel,
  app_version,
  app_build_id
HAVING
  has_fission != 1
