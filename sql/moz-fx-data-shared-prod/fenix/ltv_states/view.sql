DECLARE max_weeks INT64 DEFAULT 32;

DECLARE death_time INT64 DEFAULT 168;

DECLARE lookback INT64 DEFAULT 28;

CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.ltv_states`
AS
WITH extracted_fields AS (
  SELECT
    *,
    BIT_COUNT(
      `mozfun`.bytes.extract_bits(days_seen_bytes, -1 * lookback, lookback)
    ) AS activity_pattern,
    BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes, -1, 1)) AS active_on_this_date,
  FROM
    `moz-fx-data-shared-prod.fenix_derived.ltv_states_v1`
)
SELECT
  client_id,
  submission_date,
  STRUCT(
    mozfun.ltv.android_states_v1(
      adjust_network,
      days_since_first_seen,
      days_since_seen,
      death_time,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      max_weeks,
      first_reported_country
    ) AS android_states_v1,
    mozfun.ltv.android_states_with_paid_v1(
      adjust_network,
      days_since_first_seen,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      max_weeks,
      first_reported_country
    ) AS android_states_with_paid_v1,
    mozfun.ltv.android_states_with_paid_v2(
      adjust_network,
      days_since_first_seen,
      days_since_seen,
      death_time,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      max_weeks,
      first_reported_country
    ) AS android_states_with_paid_v2
  ) AS markov_states,
  * EXCEPT (client_id, submission_date)
FROM
  extracted_fields
