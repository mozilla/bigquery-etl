SELECT
  submission_date,
  normalized_channel,
  COUNT(DISTINCT client_id) AS nbr_unique_users
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  event_category = 'security.ui.certerror'
  AND event_object = 'aboutcerterror'
  AND event_method = 'load'
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  normalized_channel
