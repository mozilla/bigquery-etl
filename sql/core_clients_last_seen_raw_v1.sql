CREATE TEMP FUNCTION
  udf_bitmask_lowest_28() AS (0x0FFFFFFF);
CREATE TEMP FUNCTION
  udf_shift_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
	0));
CREATE TEMP FUNCTION
  udf_bits_from_days_since_created_profile(days_since_created_profile INT64) AS (
  IF
    (days_since_created_profile BETWEEN 0
      AND 6,
      1 << days_since_created_profile,
      0));
CREATE TEMP FUNCTION
  udf_shift_bits_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
    0));
CREATE TEMP FUNCTION
  udf_combine_adjacent_days_bits(prev INT64,
    curr INT64) AS (udf_shift_bits_one_day(prev) + IFNULL(curr,
    0));
CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_bits(prev INT64,
    curr INT64) AS ( COALESCE( NULLIF(udf_shift_bits_one_day(prev),
        0),
      curr,
      0));
--
  -- Equivalent to, but more efficient than, calling udf_bitmask_range(1, 28)
WITH
  _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    udf_bits_from_days_since_created_profile(
      DATE_DIFF(submission_date, profile_date, DAY)) AS days_created_profile_bits,
    * EXCEPT (submission_date)
  FROM
    core_clients_daily_v1
  WHERE
    submission_date = @submission_date ),
  --
  _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    core_clients_last_seen_raw_v1 AS cls
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf_shift_one_day(days_seen_bits) > 0)
  --
SELECT
  @submission_date AS submission_date,
IF
  (_current.client_id IS NOT NULL,
    _current,
    _previous).* REPLACE ( --
    udf_combine_adjacent_days_bits(_previous.days_seen_bits,
      _current.days_seen_bits) AS days_seen_bits,
    udf_coalesce_adjacent_days_bits(_previous.days_created_profile_bits,
      _current.days_created_profile_bits) AS days_created_profile_bits)
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
