SELECT
  DATE(submission_timestamp) AS submission_date,
  JSON_VALUE(event_extra.value) AS cert_load_error,
  COUNT(DISTINCT(client_id)) AS error_dau,
  COUNT(*) AS error_events
FROM
  `moz-fx-data-shared-prod.firefox_desktop.events_stream`
WHERE
  event_category = 'security.ui.certerror'
  AND event_name = 'load_aboutcerterror'
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  JSON_VALUE(event_extra.value)
