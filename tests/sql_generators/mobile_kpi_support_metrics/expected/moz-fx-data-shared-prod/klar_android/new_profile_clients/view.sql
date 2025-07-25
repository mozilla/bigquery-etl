-- Query generated via `mobile_kpi_support_metrics` SQL generator.
-- TODO: keeping this view as is for now until the new table get's backfilled.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.new_profile_clients`
AS
SELECT
  client_id,
  active_users.submission_date AS first_seen_date,
  active_users.normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  attribution.paid_vs_organic,
  device_type,
FROM
  `moz-fx-data-shared-prod.klar_android.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.klar_android.attribution_clients` AS attribution
  USING (client_id) --temporarily removing normalized_channel until backfill finishes
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
  AND is_daily_user
  AND active_users.submission_date = active_users.first_seen_date
