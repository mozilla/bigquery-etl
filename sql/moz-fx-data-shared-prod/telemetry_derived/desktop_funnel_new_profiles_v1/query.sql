WITH pop AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id) AS rn,
    client_id,
    normalized_country_code AS country_code,
    normalized_channel AS channel,
    application.build_id AS build_id,
    normalized_os AS os,
    environment.settings.attribution.source AS attribution_source,
    environment.partner.distribution_id AS distribution_id,
    coalesce(environment.settings.attribution.ua, '') AS attribution_ua,
    DATE(submission_timestamp) AS date
  FROM
    telemetry.new_profile
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND payload.processes.parent.scalars.startup_profile_selection_reason = 'firstrun-created-default'
)
SELECT
  date,
  channel,
  build_id,
  os,
  attribution_source,
  distribution_id,
  attribution_ua,
  country_codes.name AS country_name
FROM
  pop
LEFT JOIN
  mozdata.static.country_codes_v1 country_codes
ON
  (country_codes.code = country_code)
WHERE
  rn = 1  -- make sure that we only get one entry per client
GROUP BY
  date,
  country_name,
  channel,
  build_id,
  os,
  attribution_source,
  distribution_id,
  attribution_ua
