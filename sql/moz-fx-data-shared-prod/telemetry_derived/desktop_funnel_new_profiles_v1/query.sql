WITH pop AS (
  SELECT
    client_id,
    country AS country_code,
    normalized_channel AS channel,
    app_build_id AS build_id,
    normalized_os AS os,
    mozfun.norm.truncate_version(normalized_os_version, "minor") AS os_version,
    attribution_source,
    distribution_id,
    attribution_ua,
    first_seen_date AS date
  FROM
    telemetry.clients_first_seen_v2
  WHERE
    first_seen_date = @submission_date
    -- we will need to wait till startup_profile_selection_reason is backfilled to clients_daily before uncommenting the following line
    -- AND startup_profile_selection_reason = 'firstrun-created-default'
)
SELECT
  date,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  country_codes.name AS country_name,
  COUNT(client_id) AS new_profiles
FROM
  pop
LEFT JOIN
  `moz-fx-data-shared-prod`.static.country_codes_v1 country_codes
ON
  (country_codes.code = country_code)
GROUP BY
  date,
  country_name,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua
