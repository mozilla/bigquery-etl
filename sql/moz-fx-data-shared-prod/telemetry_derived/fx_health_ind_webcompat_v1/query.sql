SELECT
  submission_date AS submission_date,
  country,
  COUNT(DISTINCT client_id) AS nbr_unique_clients_etp_disablement
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  event_category = 'security.ui.protectionspopup'
  AND submission_date = @submission_date
  AND event_object = 'etp_toggle_off'
GROUP BY
  submission_date,
  country
