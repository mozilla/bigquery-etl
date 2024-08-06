-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.new_profile_clients`
AS
SELECT
  client_id,
  first_seen_date,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  locale,
  isp,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.klar_android.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.klar_android.attribution_clients` AS attribution
  USING (client_id)
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
