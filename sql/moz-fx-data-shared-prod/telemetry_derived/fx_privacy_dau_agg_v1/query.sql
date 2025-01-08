SELECT
  event_object,
  submission_date,
  COUNT(*) AS nbr_events,
  COUNT(DISTINCT client_id) AS nbr_distinct_users,
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  event_category = 'security.ui.protectionspopup'
  AND submission_date = @submission_date
GROUP BY
  event_object,
  submission_date
ORDER BY
  submission_date
