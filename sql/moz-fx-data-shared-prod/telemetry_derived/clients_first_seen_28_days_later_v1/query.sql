WITH clients_first_seen_28_days_ago AS (
  SELECT
    client_id,
    sample_id,
    country AS country_code,
    normalized_channel AS channel,
    app_build_id AS build_id,
    normalized_os AS os,
    mozfun.norm.truncate_version(normalized_os_version, "minor") AS os_version,
    attribution_source,
    distribution_id,
    attribution_ua,
    -- startup_profile_selection_reason, when startup_profile_selection_reason is available
    first_seen_date,
  FROM
    telemetry_derived.clients_first_seen_v2
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
),
clients_first_seen_28_days_ago_with_days_seen AS (
  SELECT
    clients_first_seen_28_days_ago.*,
    cls.days_seen_bits,
    cls.days_visited_1_uri_bits,
    cls.days_interacted_bits,
  FROM
    clients_first_seen_28_days_ago
  LEFT JOIN
    telemetry.clients_last_seen cls
  ON
    clients_first_seen_28_days_ago.client_id = cls.client_id
    AND cls.submission_date = @submission_date
)
SELECT
  client_id,
  sample_id,
  first_seen_date,
  @submission_date AS submission_date,
  country_code,
  channel,
  build_id,
  os,
  os_version,
  attribution_source,
  distribution_id,
  attribution_ua,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('1111111000000000000000000000') & days_seen_bits) >= 5,
    FALSE
  ) AS activated,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_seen_bits) > 0,
    FALSE
  ) AS returned_second_day,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0111111111111111111111111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_second_day,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_seen_bits) > 0,
    FALSE
  ) AS retained_week4,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0000000000000000000001111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_week4,
  COALESCE(
    days_seen_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_seen_bits,
  COALESCE(
    days_visited_1_uri_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_visited_1_uri_bits,
  COALESCE(
    days_interacted_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_interacted_bits,
FROM
  clients_first_seen_28_days_ago_with_days_seen
