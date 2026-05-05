SELECT
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN normalized_os_version = '6.1'
      THEN 'Win7'
    WHEN normalized_os_version = '6.2'
      THEN 'Win8'
    WHEN normalized_os_version = '6.3'
      THEN 'Win8.1'
    WHEN (normalized_os_version = '10.0' AND environment.system.os.windows_build_number >= 22000)
      THEN 'Win11'
    WHEN (normalized_os_version = '10.0' AND environment.system.os.windows_build_number < 22000)
      THEN 'Win10'
    ELSE normalized_os
  END AS os_version_bucket,
  COUNT(DISTINCT client_id) AS new_profiles
FROM
  `moz-fx-data-shared-prod.telemetry.new_profile`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  os_version_bucket
