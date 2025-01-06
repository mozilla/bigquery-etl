SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  COUNT(DISTINCT client_id) AS nbr_unique_fx_release_channel_clients_with_uninstalls
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
  AND application.channel = 'release'
GROUP BY
  submission_date,
  normalized_country_code
