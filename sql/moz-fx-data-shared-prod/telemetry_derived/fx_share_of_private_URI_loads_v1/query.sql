SELECT
  SUM(total_uri_count_private_mode) AS total_uri_count_private_mode,
  SUM(total_uri_count_normal_mode) AS total_uri_count_normal_mode,
  sample_id,
  os,
  os_version,
  country,
  submission_date
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date_s3 = @submission_date
  AND app_name = 'Firefox'
  AND total_uri_count_private_mode > 0
GROUP BY
  sample_id,
  os,
  os_version,
  country,
  submission_date
