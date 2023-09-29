-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_retention(bits INT64, submission_date DATE) AS (
  mozfun.bits28.retention(bits, submission_date)
);

-- Tests
WITH test_data AS (
  SELECT
    udf.bits28_retention((1 << 13) | (1 << 10) | 1, DATE('2020-01-28')) AS retention
)
SELECT
  mozfun.assert.equals(DATE('2020-01-28'), retention.day_0.metric_date),
  mozfun.assert.equals(DATE('2020-01-22'), retention.day_6.metric_date),
  mozfun.assert.equals(DATE('2020-01-15'), retention.day_13.metric_date),
  mozfun.assert.equals(DATE('2020-01-08'), retention.day_20.metric_date),
  mozfun.assert.equals(DATE('2020-01-01'), retention.day_27.metric_date),
  mozfun.assert.true(retention.day_0.active_on_metric_date),
  mozfun.assert.false(retention.day_6.active_on_metric_date),
  mozfun.assert.true(retention.day_13.active_on_metric_date),
  mozfun.assert.false(retention.day_20.active_on_metric_date),
  mozfun.assert.false(retention.day_27.active_on_metric_date),
  mozfun.assert.true(retention.day_13.active_in_week_0),
  mozfun.assert.true(retention.day_13.active_in_week_0_after_metric_date),
  mozfun.assert.true(retention.day_13.active_in_week_1),
  mozfun.assert.false(retention.day_20.active_in_week_0),
FROM
  test_data
