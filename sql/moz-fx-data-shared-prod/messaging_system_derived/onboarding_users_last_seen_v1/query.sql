WITH _current AS (
  SELECT
-- In this raw table, we capture the history of activity over the past
-- 28 days as a single 64-bit integer. The rightmost bit represents
-- whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    * EXCEPT (submission_date)
  FROM
    onboarding_users_daily_v1
  WHERE
    submission_date = @submission_date
),
--
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    onboarding_users_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
--
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits
  )
FROM
  _current
FULL JOIN
  _previous
  USING (client_id)
