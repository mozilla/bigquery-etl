CREATE OR REPLACE FUNCTION mozfun.ltv.get_state_desktop_v1(
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
      -- users age. 0 on their first day (we need this uniquely ID'd)
  CONCAT(
      -- we treat their first main ping date as their first day here.
    CASE
      WHEN (days_since_first_seen < max_days)
        THEN days_since_first_seen
      ELSE max_days
    END,
    '_',
      -- day of week client was acquired. allows for daily seasonality
    EXTRACT(DAYOFWEEK FROM first_seen_date),
    '_',
      -- this is redundant with the previous factor for all states EXCEPT the state at max_days - needed for daily seasonality in that case.
    EXTRACT(DAYOFWEEK FROM submission_date),
    '_',
      -- `pattern` is the number of active days in (up to) the last 364 days
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
    active
  )
);

--Tests:
SELECT
  mozfun.assert.equals(
    '5_2_7_3_1',
    mozfun.ltv.get_state_desktop_v1(50, 2, '2024-07-13', '2024-01-01', 300, 22, 1, 5, 3)
  ),
  mozfun.assert.equals(
    '5_7_7_1_1',
    mozfun.ltv.get_state_desktop_v1(100, 100, '2024-07-13', '2024-04-06', 300, 0, 1, 5, 7)
  ),
  mozfun.assert.equals(
    '5_1_7_1_1',
    mozfun.ltv.get_state_desktop_v1(400, 100, '2024-07-13', '2023-06-11', 300, 0, 1, 5, 7)
  );
