-- Params Note: Set these same values in fenix.client_ltv
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.ltv_states`
AS
WITH extracted_fields AS (
  SELECT
    *,
    BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes, -1 * 28, 28)) AS activity_pattern,
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
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      32,
      first_reported_country
    ) AS android_states_v1,
    mozfun.ltv.android_states_v2(
      adjust_network,
      days_since_first_seen,
      days_since_seen,
      168,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      32,
      first_reported_country
    ) AS android_states_v2,
    mozfun.ltv.android_states_with_paid_v1(
      adjust_network,
      days_since_first_seen,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      32,
      first_reported_country
    ) AS android_states_with_paid_v1,
    mozfun.ltv.android_states_with_paid_v2(
      adjust_network,
      days_since_first_seen,
      days_since_seen,
      168,
      submission_date,
      first_seen_date,
      activity_pattern,
      active_on_this_date,
      32,
      first_reported_country
    ) AS android_states_with_paid_v2
  ) AS markov_states,
  * EXCEPT (client_id, submission_date)
FROM
  extracted_fields
