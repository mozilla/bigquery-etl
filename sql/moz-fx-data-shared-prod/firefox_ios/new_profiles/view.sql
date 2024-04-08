CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.new_profiles`
AS
SELECT
  new_profiles.submission_date,
  new_profiles.first_seen_date,
  new_profiles.client_id,
  new_profiles.sample_id,
  new_profiles.normalized_channel AS channel,
  new_profiles.country,
  new_profiles.isp,
  new_profiles.normalized_os_version AS os_version,
  new_profiles.app_display_version AS app_version,
  new_profiles.device_manufacturer,
  (new_profiles.app_version = '107.2' AND new_profiles.submission_date >= '2023-02-01') AS is_suspicious_device_client,
  attribution.adjust_info.* EXCEPT(submission_timestamp),
FROM
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen` AS new_profiles
LEFT JOIN `moz-fx-data-shared-prod.firefox_ios.new_profile_attribution` AS attribution
  USING(client_id, first_seen_date, channel)
WHERE
  is_new_profile
