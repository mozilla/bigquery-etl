/*

Return a nested struct providing booleans indicating whether a given client
was active various time periods based on the passed bit pattern.

These booleans can then be counted across clients to provide the aggregate
numerator and denominator for the standard
1-Week, 2-Week, and 3-Week retention definitions.

Full example of usage:

WITH base AS (
  SELECT
    *,
    bits28.retention(days_seen_bits, submission_date) AS retention,
    bits28.days_since_seen(days_created_profile_bits) = 13 AS is_new_profile
  FROM
    telemetry.clients_last_seen )
SELECT
  retention.day_13.metric_date,
  -- 1-Week Retention matching GUD.
  SAFE_DIVIDE(
    COUNTIF(retention.day_13.active_in_week_1),
    COUNTIF(retention.day_13.active_in_week_0)
  ) AS retention_1_week,
  -- 1-Week New Profile Retention matching GUD.
  SAFE_DIVIDE(
    COUNTIF(is_new_profile AND retention.day_13.active_in_week_1),
    COUNTIF(is_new_profile)
  ) AS retention_1_week_new_profile,
FROM
  base
WHERE
  submission_date = '2020-01-28'
GROUP BY
  metric_date

See detailed docs for the bits28 suite of functions:
https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference

*/
CREATE OR REPLACE FUNCTION bits28.retention(bits INT64, submission_date DATE) AS (
  STRUCT(
    STRUCT(
      submission_date AS metric_date,
      bits28.active_in_range(bits, 0, 1) AS active_on_metric_date
    ) AS day_0,
    STRUCT(
      DATE_SUB(submission_date, INTERVAL 6 DAY) AS metric_date,
      bits28.active_in_range(bits, -6, 1) AS active_on_metric_date,
      bits28.active_in_range(bits, -6, 7) AS active_in_week_0,
      bits28.active_in_range(bits, -5, 6) AS active_in_week_0_after_metric_date
    ) AS day_6,
    STRUCT(
      DATE_SUB(submission_date, INTERVAL 13 DAY) AS metric_date,
      bits28.active_in_range(bits, -13, 1) AS active_on_metric_date,
      bits28.active_in_range(bits, -13, 7) AS active_in_week_0,
      bits28.active_in_range(bits, -12, 6) AS active_in_week_0_after_metric_date,
      bits28.active_in_range(bits, -6, 7) AS active_in_week_1
    ) AS day_13,
    STRUCT(
      DATE_SUB(submission_date, INTERVAL 20 DAY) AS metric_date,
      bits28.active_in_range(bits, -20, 1) AS active_on_metric_date,
      bits28.active_in_range(bits, -20, 7) AS active_in_week_0,
      bits28.active_in_range(bits, -19, 6) AS active_in_week_0_after_metric_date,
      bits28.active_in_range(bits, -13, 7) AS active_in_week_1,
      bits28.active_in_range(bits, -6, 7) AS active_in_week_2
    ) AS day_20,
    STRUCT(
      DATE_SUB(submission_date, INTERVAL 27 DAY) AS metric_date,
      bits28.active_in_range(bits, -27, 1) AS active_on_metric_date,
      bits28.active_in_range(bits, -27, 7) AS active_in_week_0,
      bits28.active_in_range(bits, -26, 6) AS active_in_week_0_after_metric_date,
      bits28.active_in_range(bits, -20, 7) AS active_in_week_1,
      bits28.active_in_range(bits, -13, 7) AS active_in_week_2,
      bits28.active_in_range(bits, -6, 7) AS active_in_week_3
    ) AS day_27
  )
);

-- Tests
WITH test_data AS (
  SELECT
    bits28.retention((1 << 13) | (1 << 10) | 1, DATE('2020-01-28')) AS retention
)
SELECT
  assert.equals(DATE('2020-01-28'), retention.day_0.metric_date),
  assert.equals(DATE('2020-01-22'), retention.day_6.metric_date),
  assert.equals(DATE('2020-01-15'), retention.day_13.metric_date),
  assert.equals(DATE('2020-01-08'), retention.day_20.metric_date),
  assert.equals(DATE('2020-01-01'), retention.day_27.metric_date),
  assert.true(retention.day_0.active_on_metric_date),
  assert.false(retention.day_6.active_on_metric_date),
  assert.true(retention.day_13.active_on_metric_date),
  assert.false(retention.day_20.active_on_metric_date),
  assert.false(retention.day_27.active_on_metric_date),
  assert.true(retention.day_13.active_in_week_0),
  assert.true(retention.day_13.active_in_week_0_after_metric_date),
  assert.true(retention.day_13.active_in_week_1),
  assert.false(retention.day_20.active_in_week_0),
FROM
  test_data
