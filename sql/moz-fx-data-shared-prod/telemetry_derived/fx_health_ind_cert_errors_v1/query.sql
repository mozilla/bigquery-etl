SELECT
  submission_date,
  event_string_value AS cert_load_error,
  COUNT(DISTINCT(client_id)) AS error_dau,
  COUNT(*) AS error_events
FROM
  `moz-fx-data-shared-prod.telemetry.events_1pct`
WHERE
  event_category = 'security.ui.certerror'
  AND event_object = 'aboutcerterror'
  AND event_method = 'load'
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  event_string_value
