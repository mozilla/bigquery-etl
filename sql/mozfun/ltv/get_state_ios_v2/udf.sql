CREATE OR REPLACE FUNCTION ltv.get_state_ios_v2(
  days_since_first_seen INT64,
  days_since_seen INT64,
  submission_date DATE,
  death_time INT64,
  pattern INT64,
  active INT64,
  max_weeks INT64
)
RETURNS STRING AS (
   -- users age. 0 on their first day (we need this uniquely ID'd) and then number of the week (1-indexed) since they were new.
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
     -- `pattern` is the number of active days in the last 28 days.
     -- users are binned into equal sized activity levels. 0 would be inactive.
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
      ELSE CAST(pattern AS string)
    END,
    '_',
    active
  )
);

--Tests
SELECT
  mozfun.assert.equals('2_dow4_2_3', ltv.get_state_ios_v2(10, 2, '2024-03-27', 300, 10, 3, 3)),
  mozfun.assert.equals('54_dow4_00_0', ltv.get_state_ios_v2(500, 436, '2024-03-27', 300, 0, 0, 54)),
  mozfun.assert.equals('4_dow4_1_22', ltv.get_state_ios_v2(28, 1, '2024-03-27', 300, 3, 22, 4))
