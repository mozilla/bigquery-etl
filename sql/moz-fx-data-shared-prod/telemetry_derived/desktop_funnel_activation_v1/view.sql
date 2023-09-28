CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.desktop_funnel_activation_v1`
AS
SELECT
  DATE_SUB(submission_date, INTERVAL 27 day) AS date,
  country_name,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  num_activated,
  returned_second_day,
  qualified_second_day,
  returned_week4,
  qualified_week4
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_funnel_activation_day_6_v1`
