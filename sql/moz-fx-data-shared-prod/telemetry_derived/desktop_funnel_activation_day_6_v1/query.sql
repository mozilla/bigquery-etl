WITH pop AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id) AS rn,
    client_id,
    normalized_country_code AS country_code,
    normalized_channel AS channel,
    application.build_id AS build_id,
    normalized_os AS os,
    mozfun.norm.truncate_version(normalized_os_version, "minor") AS os_version,
    environment.settings.attribution.source AS attribution_source,
    environment.partner.distribution_id AS distribution_id,
    COALESCE(environment.settings.attribution.ua, '') AS attribution_ua,
    DATE(submission_timestamp) AS date
  FROM
    telemetry.new_profile
  WHERE
    DATE(submission_timestamp) = DATE_SUB(@submission_date, INTERVAL 6 day)
    AND payload.processes.parent.scalars.startup_profile_selection_reason = 'firstrun-created-default'
),
dist_pop AS (
  -- make sure that we only get one entry per client
  SELECT
    * EXCEPT (rn)
  FROM
    pop
  WHERE
    rn = 1
),
dist_pop_with_days_seen AS (
  SELECT
    a.*,
    b.days_seen_bits
  FROM
    dist_pop a
  LEFT JOIN
    telemetry.clients_last_seen b
    ON (a.client_id = b.client_id)
  WHERE
    b.submission_date = @submission_date
),
client_conditions AS (
  SELECT
    client_id,
    date,
    country_code,
    channel,
    build_id,
    os,
    os_version,
    attribution_source,
    distribution_id,
    attribution_ua,
    COALESCE(udf.bitcount_lowest_7(days_seen_bits), 0) >= 5 AS activated
  FROM
    dist_pop_with_days_seen
)
SELECT
  DATE(@submission_date) AS submission_date,
  country_codes.name AS country_name,
  channel,
  build_id,
  os,
  CAST(os_version AS STRING) AS os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  COUNT(*) AS num_activated
FROM
  client_conditions
LEFT JOIN
  `moz-fx-data-shared-prod`.static.country_codes_v1 country_codes
  ON (country_codes.code = country_code)
WHERE
  activated = TRUE
GROUP BY
  submission_date,
  country_name,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua
