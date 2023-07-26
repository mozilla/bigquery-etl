WITH base AS (
  -- There are duplicates now in `baseline_clients_daily` because of the join with `clients_first_seen`,
  -- so we take the minimum
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        first_seen_date ASC
        --first_run_date ASC -- Don't have a first run_date in regen_sim_replaced_baseline_clients_daily_14142222
    ) AS rn,
  FROM
    `mozdata.analysis.regen_sim_replaced_baseline_clients_daily_14142222`
  WHERE
    submission_date = @submission_date
),
_current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 365 days for each usage criterion as an array of bytes. The
    -- rightmost bit represents whether the user was active in the current day.
    * EXCEPT (submission_date, rn),
    udf.bool_to_365_bits(TRUE) AS days_seen_bytes,
  FROM
    base
  WHERE
    rn = 1
),
_previous AS (
  SELECT
    * EXCEPT (submission_date),
  FROM
    analysis.regen_sim_replaced_clients_yearly_14142222
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
    AND BIT_COUNT(udf.shift_365_bits_one_day(days_seen_bytes)) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_365_bits(
      _previous.days_seen_bytes,
      _current.days_seen_bytes
    ) AS days_seen_bytes
  )
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
  (sample_id, client_id)
