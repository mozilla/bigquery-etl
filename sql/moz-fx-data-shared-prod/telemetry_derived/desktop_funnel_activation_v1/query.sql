WITH distinct_countries AS (
  -- Some country codes appear multiple times as some countries have multiple names.
  -- Ensure that each code appears only once and go with name that appears first.
  SELECT
    code,
    name
  FROM
    (
      SELECT
        row_number() OVER (PARTITION BY code ORDER BY name) AS rn,
        code,
        name
      FROM
        `moz-fx-data-derived-datasets`.static.country_names_v1 country_names
    )
  WHERE
    rn = 1
),
pop AS (
  SELECT DISTINCT
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
raw_info AS (
  SELECT
    client_id,
    submission_date,
    any_value(days_seen_bits) AS days_seen_bits
  FROM
    telemetry.clients_last_seen
  WHERE
    submission_date
    BETWEEN @submission_date
    AND DATE_ADD(@submission_date, INTERVAL 8 DAY)
  GROUP BY
    client_id,
    submission_date
),
eight_days_later AS (
  SELECT
    a.*,
    b.days_seen_bits
  FROM
    pop a
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
  country_names.name AS country_name,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  count(*) AS num_activated
FROM
  client_conditions
LEFT JOIN
  distinct_countries country_names
ON
  (country_names.code = country_code)
WHERE
  date = @submission_date
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
