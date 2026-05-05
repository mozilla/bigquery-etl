SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  COUNT(
    DISTINCT
    CASE
      WHEN application.channel = 'release'
        THEN client_id
      ELSE NULL
    END
  ) AS nbr_unique_fx_release_channel_clients_with_uninstalls,
  AVG(
    CASE
      WHEN application.channel = 'release'
        THEN environment.profile.creation_date
      ELSE NULL
    END
  ) AS avg_profile_creation_date,
  COUNT(DISTINCT(client_id)) AS nbr_unique_clients_with_uninstalls
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  normalized_country_code
