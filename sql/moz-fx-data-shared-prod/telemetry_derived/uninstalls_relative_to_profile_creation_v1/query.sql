SELECT
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN DATE(creation_date) = DATE(submission_timestamp)
      THEN 'profile created the same day'
    WHEN DATE(creation_date) >= DATE_SUB(DATE(submission_timestamp), INTERVAL 2 DAY)
      THEN 'profile created the day before'
    WHEN DATE(creation_date) >= DATE_SUB(DATE(submission_timestamp), INTERVAL 14 DAY)
      THEN 'profile created in last 2 weeks'
    WHEN DATE(creation_date) >= DATE_SUB(DATE(submission_timestamp), INTERVAL 365 DAY)
      THEN 'profile created in last year'
    WHEN DATE(creation_date) >= DATE_SUB(DATE(submission_timestamp), INTERVAL 1095 DAY)
      THEN 'profile created in last 3 years'
    WHEN DATE(creation_date) > DATE(submission_timestamp)
      THEN 'odd'
    ELSE 'other'
  END AS profile_age_at_time_of_uninstall,
  COUNT(DISTINCT client_id) AS nbr_clients_uninstalling,
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  profile_age_at_time_of_uninstall
