CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_clients`
AS
SELECT
  new_clients.submission_date,
  new_clients.first_seen_date,
  new_clients.client_id,
  new_clients.sample_id,
  new_clients.normalized_channel AS channel,
  new_clients.country,
  new_clients.isp,
  new_clients.normalized_os_version AS os_version,
  new_clients.app_display_version AS app_version,
  new_clients.device_manufacturer,
  (new_clients.app_version = '107.2' AND new_clients.submission_date >= '2023-02-01') AS is_suspicious_device_client,
  attribution.adjust_info.* EXCEPT(submission_timestamp),
FROM
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen` AS new_clients
LEFT JOIN `moz-fx-data-shared-prod.firefox_ios.new_clients_attribution` AS attribution
  USING(client_id, first_seen_date, channel)
WHERE
  is_new_profile
