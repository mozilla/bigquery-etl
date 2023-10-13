CREATE OR REPLACE FUNCTION udf.active_n_weeks_ago(x INT64, n INT64)
RETURNS BOOLEAN AS (
  BIT_COUNT(x >> (7 * n) & udf.bitmask_lowest_7()) > 0
);

-- Tests
SELECT
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 0, 0)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 6, 0)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 7, 1)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 13, 1)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 14, 2)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 20, 2)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 21, 3)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 << 27, 3)),
  mozfun.assert.true(udf.active_n_weeks_ago(1 + (1 << 9), 1)),
  mozfun.assert.false(udf.active_n_weeks_ago(0, 0)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 6, 1)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 7, 0)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 13, 2)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 14, 1)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 20, 3)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 21, 2)),
  mozfun.assert.false(udf.active_n_weeks_ago(1 << 27, 4)),
  mozfun.assert.null(udf.active_n_weeks_ago(NULL, 0)),
  TRUE
