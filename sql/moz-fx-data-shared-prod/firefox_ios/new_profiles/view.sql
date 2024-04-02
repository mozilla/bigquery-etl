CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profiles`
AS
SELECT
  submission_date,
  first_seen_date,
  client_id,
  sample_id,
  normalized_channel AS channel,
  country,
  isp,
  normalized_os_version AS os_version,
  app_display_version AS app_version,
  device_manufacturer,
  (app_version = '107.2' AND submission_date >= '2023-02-01') AS is_suspicious_device_client,
  -- TODO: add attribution once attribution table exists
FROM
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen`
WHERE
  is_new_profile
