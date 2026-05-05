/*
  LTV states for Android. Results in strings like:
  "1_dow3_organic_2_1" and "0_dow1_paid_1_1"

  Compared to V1, these states have a "dead" state, determined by "death_time".
  The model can use this state as a sink, where the client will never return if they are dead.
*/
CREATE OR REPLACE FUNCTION ltv.android_states_with_paid_v2(
  adjust_network STRING,
  days_since_first_seen INT64,
  days_since_seen INT64,
  death_time INT64,
  submission_date DATE,
  first_seen_date DATE,
  pattern INT64,
  active INT64,
  max_weeks INT64,
  country STRING
)
RETURNS STRING AS (
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
    '_dow',
      -- day of week. allows the model to capture weekly seasonality
    EXTRACT(DAYOFWEEK FROM submission_date),
    '_',
      -- whether the client is paid or organic
    CASE
      WHEN adjust_network = "Google Ads ACI"
        THEN 'paid'
      ELSE 'organic'
    END,
    '_',
      -- `pattern` is the number of active days in the last 28 days.
      -- users are binned into equal sized activity levels. 0 would be inactive.
      -- 00 would be gone at least 100 days
    CASE
      WHEN days_since_seen >= death_time
        THEN '00'
      WHEN pattern
        BETWEEN 1
        AND 7
        THEN '1'
      WHEN pattern
        BETWEEN 8
        AND 14
        THEN '2'
      WHEN pattern
        BETWEEN 15
        AND 21
        THEN '3'
      WHEN pattern > 21
        THEN '4'
      ELSE '0'
    END,
    '_',
    active
  )
);

-- Tests
SELECT
  assert.equals(
    "0_dow1_organic_1_1",
    ltv.android_states_with_paid_v2(
      "abc",
      0,
      0,
      100,
      DATE("2023-01-01"),
      DATE("2023-01-01"),
      1,
      1,
      28,
      "US"
    )
  ),
  assert.equals(
    "1_dow2_paid_1_0",
    ltv.android_states_with_paid_v2(
      "Google Ads ACI",
      1,
      1,
      100,
      DATE("2023-01-02"),
      DATE("2023-01-01"),
      2,
      0,
      28,
      "US"
    )
  ),
  assert.equals(
    "28_dow2_paid_00_0",
    ltv.android_states_with_paid_v2(
      "Google Ads ACI",
      365,
      365,
      100,
      DATE("2024-01-01"),
      DATE("2023-01-01"),
      365,
      0,
      28,
      "US"
    )
  )
