WITH _current AS (
  SELECT
    -- One and only one of impression_id or client_id will be null in each row;
    -- nulls are not considered equivalent in joins, so we explicitly need to
    -- construct a join key column from the non-null value.
    COALESCE(impression_id, client_id) AS _join_key,
    --
    -- In this raw table, we capture the history of activity over the past
    -- 28 days as a single 64-bit integer. The rightmost bit represents
    -- whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    CAST(seen_whats_new AS INT64) AS days_seen_whats_new_bits,
    * EXCEPT (submission_date)
  FROM
    cfr_users_daily_v1
  WHERE
    submission_date = @submission_date
),
--
_previous AS (
  SELECT
    COALESCE(impression_id, client_id) AS _join_key,
    * EXCEPT (submission_date)
  FROM
    cfr_users_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
--
SELECT
  @submission_date AS submission_date,
  IF(_current._join_key IS NOT NULL, _current, _previous).* EXCEPT (_join_key) REPLACE(
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_whats_new_bits,
      _current.days_seen_whats_new_bits
    ) AS days_seen_whats_new_bits
  )
FROM
  _current
FULL JOIN
  _previous
USING
  (_join_key)
