CREATE TEMP FUNCTION
  udf_bitmask_lowest_28() AS (0x0FFFFFFF);
CREATE TEMP FUNCTION
  udf_shift_bits_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
    0));
CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_bits(prev INT64,
    curr INT64) AS ( COALESCE( NULLIF(udf_shift_bits_one_day(prev),
        0),
      curr,
      0));
CREATE TEMP FUNCTION
  udf_combine_adjacent_days_bits(prev INT64,
    curr INT64) AS (udf_shift_bits_one_day(prev) + IFNULL(curr,
    0));
--
WITH
  _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- Record days on which the user was in a "Tier 1" country;
    -- this allows a variant of country-segmented MAU where we can still count
    -- a user that appeared in one of the target countries in the previous
    -- 28 days even if the most recent "country" value is not in this set.
    CAST(seen_in_tier1_country AS INT64) AS days_seen_in_tier1_country_bits,
    CAST(registered AS INT64) AS days_registered_bits,
    * EXCEPT (submission_date, seen_in_tier1_country, registered, monitor_only),
    CAST(NOT monitor_only AS INT64) AS days_seen_no_monitor_bits
  FROM
    fxa_users_daily_v1
  WHERE
    submission_date = @submission_date ),
  _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    fxa_users_last_seen_raw_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf_shift_bits_one_day(days_seen_bits) > 0)
SELECT
  @submission_date AS submission_date,
IF
  (_current.user_id IS NOT NULL,
    _current,
    _previous).* REPLACE ( --
    udf_combine_adjacent_days_bits(_previous.days_seen_bits,
      _current.days_seen_bits) AS days_seen_bits,
    udf_combine_adjacent_days_bits(_previous.days_seen_in_tier1_country_bits,
      _current.days_seen_in_tier1_country_bits) AS days_seen_in_tier1_country_bits,
    udf_coalesce_adjacent_days_bits(_previous.days_registered_bits,
      _current.days_registered_bits) AS days_registered_bits,
    udf_combine_adjacent_days_bits(_previous.days_seen_no_monitor_bits,
      _current.days_seen_no_monitor_bits) AS days_seen_no_monitor_bits )
FROM
  _current
FULL JOIN
  _previous
USING
  (user_id)
