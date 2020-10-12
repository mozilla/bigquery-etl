CREATE OR REPLACE FUNCTION udf.active_n_weeks_ago(x INT64, n INT64)
RETURNS BOOLEAN AS (
  BIT_COUNT(x >> (7 * n) & udf.bitmask_lowest_7()) > 0
);

-- Tests
SELECT
  assert.true(udf.active_n_weeks_ago(1 << 0, 0)),
  assert.true(udf.active_n_weeks_ago(1 << 6, 0)),
  assert.true(udf.active_n_weeks_ago(1 << 7, 1)),
  assert.true(udf.active_n_weeks_ago(1 << 13, 1)),
  assert.true(udf.active_n_weeks_ago(1 << 14, 2)),
  assert.true(udf.active_n_weeks_ago(1 << 20, 2)),
  assert.true(udf.active_n_weeks_ago(1 << 21, 3)),
  assert.true(udf.active_n_weeks_ago(1 << 27, 3)),
  assert.true(udf.active_n_weeks_ago(1 + (1 << 9), 1)),
  assert.false(udf.active_n_weeks_ago(0, 0)),
  assert.false(udf.active_n_weeks_ago(1 << 6, 1)),
  assert.false(udf.active_n_weeks_ago(1 << 7, 0)),
  assert.false(udf.active_n_weeks_ago(1 << 13, 2)),
  assert.false(udf.active_n_weeks_ago(1 << 14, 1)),
  assert.false(udf.active_n_weeks_ago(1 << 20, 3)),
  assert.false(udf.active_n_weeks_ago(1 << 21, 2)),
  assert.false(udf.active_n_weeks_ago(1 << 27, 4)),
  assert.null(udf.active_n_weeks_ago(NULL, 0)),
  TRUE
