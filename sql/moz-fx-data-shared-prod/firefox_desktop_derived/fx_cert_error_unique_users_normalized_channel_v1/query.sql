SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_channel,
  COUNT(DISTINCT client_id) AS nbr_unique_users
FROM
  `moz-fx-data-shared-prod.firefox_desktop.events_stream`
WHERE
  event_category = 'security.ui.certerror'
  AND event_name = 'load_aboutcerterror'
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  DATE(submission_timestamp),
  normalized_channel
