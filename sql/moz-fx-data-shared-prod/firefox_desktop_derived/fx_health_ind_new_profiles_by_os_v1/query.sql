SELECT
  submission_date,
  CASE
    WHEN normalized_os_version = '6.1'
      THEN 'Win7'
    WHEN normalized_os_version = '6.2'
      THEN 'Win8'
    WHEN normalized_os_version = '6.3'
      THEN 'Win8.1'
    WHEN (normalized_os_version = '10.0' AND windows_build_number >= 22000)
      THEN 'Win11'
    WHEN (normalized_os_version = '10.0' AND windows_build_number < 22000)
      THEN 'Win10'
    ELSE normalized_os
  END AS os_version_bucket,
  COUNT(DISTINCT(client_id)) AS new_profiles
FROM
  `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen`
WHERE
  first_seen_date = @submission_date
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  os_version_bucket
