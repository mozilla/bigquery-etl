SELECT
  DATE(submission_timestamp) AS date,
  country_names.name AS country_name,
  normalized_channel AS channel,
  application.build_id AS build_id,
  normalized_os AS os,
  environment.settings.attribution.source AS attribution_source,
  environment.partner.distribution_id AS distribution_id,
  coalesce(environment.settings.attribution.ua, '') AS attribution_ua,
  COUNT(DISTINCT client_id) AS new_profiles,
FROM
  telemetry.new_profile
LEFT JOIN
  `moz-fx-data-derived-datasets`.static.country_names_v1 country_names
ON
  (country_names.code = normalized_country_code)
WHERE
  DATE(submission_timestamp) = @submission_date
  AND payload.processes.parent.scalars.startup_profile_selection_reason = 'firstrun-created-default'
GROUP BY
  date,
  country_name,
  channel,
  build_id,
  os,
  attribution_source,
  distribution_id,
  attribution_ua
