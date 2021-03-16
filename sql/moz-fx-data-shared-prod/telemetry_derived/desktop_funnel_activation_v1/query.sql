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
    coalesce(environment.settings.attribution.ua, '') AS attribution_ua,
    DATE(submission_timestamp) AS date
  FROM
    telemetry.new_profile
  WHERE
    DATE(submission_timestamp) = @submission_date
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
raw_info AS (
  SELECT
    client_id,
    submission_date,
    days_seen_bits
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 8 DAY)
    AND @submission_date
),
eight_days_later AS (
  SELECT
    a.*,
    b.days_seen_bits
  FROM
    dist_pop a
  LEFT JOIN
    raw_info b
  ON
    (a.client_id = b.client_id AND DATE_DIFF(b.submission_date, a.date, DAY) = 6)
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
    coalesce(udf.bitcount_lowest_7(days_seen_bits), 0) >= 5 AS activated
  FROM
    eight_days_later
)
SELECT
  date,
  country_codes.name AS country_name,
  channel,
  build_id,
  os,
  CAST(os_version AS STRING) AS os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  count(*) AS num_activated
FROM
  client_conditions
LEFT JOIN
  mozdata.static.country_codes_v1 country_codes
ON
  (country_codes.code = country_code)
WHERE
  date = DATE_SUB(@submission_date, INTERVAL 8 DAY)
  AND activated = TRUE
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
