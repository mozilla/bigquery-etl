SELECT
  submission_date,
  sample_id,
  os,
  os_version,
  country,
  IF(total_uri_count_private_mode > 0, TRUE, FALSE) AS client_used_private_mode,
  IF(total_uri_count_private_mode < 0, TRUE, FALSE) AS client_had_negative_uri_count_private,
  IF(total_uri_count_normal_mode < 0, TRUE, FALSE) AS client_negative_uri_count_normal,
  SUM(total_uri_count_private_mode) AS total_uri_count_private_mode,
  SUM(total_uri_count_normal_mode) AS total_uri_count_normal_mode
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  sample_id,
  os,
  os_version,
  country,
  client_used_private_mode,
  client_had_negative_uri_count_private,
  client_negative_uri_count_normal
