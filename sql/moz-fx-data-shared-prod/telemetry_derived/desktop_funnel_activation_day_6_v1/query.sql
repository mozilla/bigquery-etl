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
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
    -- we will need to wait till startup_profile_selection_reason is backfilled to clients_daily before uncommenting the following line
    -- AND startup_profile_selection_reason = 'firstrun-created-default'
),

dist_pop_with_days_seen AS (
  SELECT
    a.*,
    b.days_seen_bits,
    b.days_visited_1_uri_bits,
    b.days_interacted_bits,
  FROM
    pop a
  LEFT JOIN
    telemetry.clients_last_seen b
  ON
    (a.client_id = b.client_id)
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
    -- did the profile send a main ping in at least 5 of their first 7 days?
    COALESCE(BIT_COUNT(mozfun.bits28.from_string('1111111000000000000000000000') & days_seen_bits) >= 5, FALSE) AS activated,
    -- did the profile send a main ping on any day after their first day during their first 28 days?
    COALESCE(BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_seen_bits) > 0, FALSE) AS returned_second_day,
    -- did the profile qualify as DAU on any day after their first day during their first 28 days?
    COALESCE(BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_visited_1_uri_bits & days_interacted_bits) > 0, FALSE) AS qualified_second_day,
    -- did the profile send a main ping on any day in their 4th week?
    COALESCE(BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_seen_bits) > 0, FALSE) AS retained_week4,
    -- did the profile qualify as DAU on any day in their 4th week?
    COALESCE(BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_visited_1_uri_bits & days_interacted_bits) > 0, FALSE) AS qualified_week4
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
  COUNTIF(activated) AS num_activated,
  COUNTIF(returned_second_day) AS returned_second_day,
  COUNTIF(qualified_second_day) AS qualified_second_day,
  COUNTIF(returned_week4) AS returned_week4,
  COUNTIF(qualified_week4) AS qualified_week4,
FROM
  client_conditions
LEFT JOIN
  `moz-fx-data-shared-prod`.static.country_codes_v1 country_codes
ON
  (country_codes.code = country_code)
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
