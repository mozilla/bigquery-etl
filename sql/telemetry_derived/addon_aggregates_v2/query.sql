SELECT
  DATE(submission_timestamp) AS submission_date,
  sample_id,
  client_id,
  normalized_channel,
  SUBSTR(application.version, 1, 2) AS app_version,
  environment.settings.locale,
  COUNTIF(is_self_install) AS n_self_installed_addons,
  COUNTIF(key LIKE "%@shield.mozilla%") AS n_shield_addons,
  COUNTIF(value.foreign_install > 0) AS n_foreign_installed_addons,
  COUNTIF(value.is_system) AS n_system_addons,
  COUNTIF(value.is_web_extension) AS n_web_extensions,
  MIN(
    IF(is_self_install, FORMAT_DATE("%Y%m%d", SAFE.DATE_FROM_UNIX_DATE(value.install_day)), NULL)
  ) AS first_addon_install_date,
  FORMAT_DATE(
    "%Y%m%d",
    SAFE.DATE_FROM_UNIX_DATE(MIN(SAFE_CAST(environment.profile.creation_date AS INT64)))
  ) AS profile_creation_date
FROM
  telemetry.main,
  UNNEST(environment.addons.active_addons),
  UNNEST(
    [
      key IS NOT NULL
      AND NOT value.is_system
      AND NOT IFNULL(value.foreign_install, 0) > 0
      AND NOT key LIKE '%mozilla%'
      AND NOT key LIKE '%cliqz%'
      AND NOT key LIKE '%@unified-urlbar%'
    ]
  ) AS is_self_install
WHERE
  client_id IS NOT NULL
  AND normalized_app_name = 'Firefox'
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  sample_id,
  client_id,
  normalized_channel,
  app_version,
  locale
