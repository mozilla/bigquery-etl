/*
  LTV states for Desktop. Results in strings like: "0_1_1_1_1" where each component is described below.
*/
CREATE OR REPLACE FUNCTION ltv.desktop_states_v1(
  days_since_first_seen INT64,
  days_since_active INT64,
  submission_date DATE,
  first_seen_date DATE,
  death_time INT64,
  pattern INT64,
  active INT64,
  max_days INT64,
  lookback INT64
)
RETURNS STRING AS (
      -- first component: users age. 0 on their first day (we need this uniquely ID'd)
  CONCAT(
    CASE
      WHEN (days_since_first_seen < max_days)
        THEN days_since_first_seen
      ELSE max_days
    END,
    '_',
      -- second component: day of week client was acquired. allows for daily seasonality
    EXTRACT(DAYOFWEEK FROM first_seen_date),
    '_',
      -- third component: current day of week.
      -- this is redundant with the previous factor for all states EXCEPT the state at max_days - needed for daily seasonality in that case.
    EXTRACT(DAYOFWEEK FROM submission_date),
    '_',
      -- fourth component: `pattern` is the number of active days in last `lookback` number of days
      -- we compute the fraction of days seen divided by the lifetime of the client, and divide into 4 activity levels
      -- with a special state if the client is gone for >= death time
    CASE
      WHEN days_since_active >= death_time
        THEN '00'
      WHEN pattern < (lookback * 1.0 / 3.0)
        THEN '1'
      WHEN pattern
        BETWEEN (lookback * 1.0 / 3.0)
        AND (lookback * 2.0 / 3.0)
        THEN '2'
      WHEN pattern > (lookback * 2.0 / 3.0)
        THEN '3'
      ELSE '0'
    END,
    '_',
      -- fifth component: whether the client is active on the current day
    active
  )
);

-- Tests
SELECT
  assert.equals(
    "0_1_1_1_1",
    ltv.desktop_states_v1(0, 0, DATE("2024-06-30"), DATE("2024-06-30"), 160, 1, 1, 168, 28)
  ),
  assert.equals(
    "168_1_1_3_1",
    ltv.desktop_states_v1(169, 0, DATE("2024-06-30"), DATE("2024-06-30"), 160, 22, 1, 168, 28)
  ),
  assert.equals(
    "168_1_1_00_0",
    ltv.desktop_states_v1(200, 169, DATE("2024-06-30"), DATE("2024-06-30"), 168, 0, 0, 168, 28)
  )
