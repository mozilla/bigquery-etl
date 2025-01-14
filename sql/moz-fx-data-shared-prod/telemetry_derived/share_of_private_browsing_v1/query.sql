SELECT
  submission_date_s3 AS submission_date,
  IF(total_uri_count_private_mode > 0, TRUE, FALSE) AS client_used_private_mode,
  IF(total_uri_count_private_mode < 0, TRUE, FALSE) AS client_had_negative_uri_count_private,
  IF(total_uri_count_normal_mode < 0, TRUE, FALSE) AS client_negative_uri_count_normal,
  COUNT(DISTINCT(client_id)) AS nbr_clients,
  SUM(total_uri_count_private_mode) AS total_uri_count_private_mode,
  SUM(total_uri_count_normal_mode) AS total_uri_count_normal_mode
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date_s3 = @submission_date
  AND app_name = 'Firefox'
GROUP BY
  submission_date_s3,
  client_used_private_mode,
  client_had_negative_uri_count_private,
  client_negative_uri_count_normal
