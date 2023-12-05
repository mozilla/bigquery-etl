/*
  LTV states for Android. Results in strings like:
  "1_dow3_2_1" and "0_dow1_1_1"
*/
CREATE OR REPLACE FUNCTION ltv.android_states_v1(
  adjust_network STRING,
  days_since_first_seen INT64,
  submission_date DATE,
  first_seen_date DATE,
  pattern INT64,
  active INT64,
  max_weeks INT64,
  country STRING
) AS (
    -- Client's age. 0 on their first day (we need this uniquely ID'd) and then number of the week (1-indexed) since they were new.
    -- their second week starts on day 8, etc.
  CONCAT(
    CASE
      WHEN days_since_first_seen = 0
        THEN 0
      WHEN (days_since_first_seen != 0)
        AND (((days_since_first_seen + 1) / 7) < max_weeks)
        THEN CEILING((days_since_first_seen + 1) / 7)
      ELSE max_weeks
    END,
      -- day of week for weekly seasonality
    '_dow',
    EXTRACT(DAYOFWEEK FROM submission_date),
    '_',
      -- users go into one of four activity levels based on last 28 days of activity
    CASE
      WHEN pattern
        BETWEEN 1
        AND 7
        THEN 1
      WHEN pattern
        BETWEEN 8
        AND 14
        THEN 2
      WHEN pattern
        BETWEEN 15
        AND 21
        THEN 3
      WHEN pattern > 21
        THEN 4
      ELSE pattern
    END,
    '_',
    active
  )
);

-- Tests
SELECT
  assert.equals(
    "0_dow1_1_1",
    ltv.android_states_v1("abc", 0, DATE("2023-01-01"), DATE("2023-01-01"), 1, 1, 28, "US")
  ),
  assert.equals(
    "1_dow2_1_0",
    ltv.android_states_v1("abc", 1, DATE("2023-01-02"), DATE("2023-01-01"), 2, 0, 28, "US")
  ),
