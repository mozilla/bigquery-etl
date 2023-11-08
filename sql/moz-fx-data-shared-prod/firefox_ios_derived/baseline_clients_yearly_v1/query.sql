WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 365 days for each usage criterion as an array of bytes. The
    -- rightmost bit represents whether the user was active in the current day.
    udf.bool_to_365_bits(TRUE) AS days_seen_bytes,
    udf.bool_to_365_bits(durations > 0) AS days_active_bytes,
    * EXCEPT (submission_date),
  FROM
    `moz-fx-data-shared-prod`.firefox_ios.baseline_clients_daily
  WHERE
    submission_date = @submission_date
    AND sample_id IS NOT NULL
),
  --
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    `moz-fx-data-shared-prod`.firefox_ios_derived.baseline_clients_yearly_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
    AND BIT_COUNT(udf.shift_365_bits_one_day(days_seen_bytes)) > 0
    AND sample_id IS NOT NULL
)
  --
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_365_bits(
      _previous.days_seen_bytes,
      _current.days_seen_bytes
    ) AS days_seen_bytes,
    udf.combine_adjacent_days_365_bits(
      _previous.days_active_bytes,
      _current.days_active_bytes
    ) AS days_active_bytes
  )
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
