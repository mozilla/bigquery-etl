SELECT
  DATE(submission_timestamp) AS submission_date,
  metadata.geo.country AS country,
  COUNT(DISTINCT(client_id)) AS nbr_unique_clients_etp_disablement
FROM
  `moz-fx-data-shared-prod.firefox_desktop.events_stream` AS e
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event = 'security.ui.protectionspopup.click_etp_toggle_off'
GROUP BY
  submission_date,
  country
