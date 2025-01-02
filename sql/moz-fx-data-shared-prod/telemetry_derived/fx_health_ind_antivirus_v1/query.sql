SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.system.sec.antivirus AS antivirus_name,
  COUNT(DISTINCT client_id) AS dau
FROM
  `moz-fx-data-shared-prod.telemetry.main_1pct`
WHERE
  normalized_os = 'Windows'
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  antivirus_name
