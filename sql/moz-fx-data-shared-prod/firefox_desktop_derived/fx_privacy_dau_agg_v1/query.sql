SELECT
  event_name AS event_object,
  DATE(submission_timestamp) AS submission_date,
  COUNT(*) AS nbr_events,
  COUNT(DISTINCT client_id) AS nbr_distinct_users,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.events_stream`
WHERE
  event_category = 'security.ui.protectionspopup'
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  event_name,
  DATE(submission_timestamp)
