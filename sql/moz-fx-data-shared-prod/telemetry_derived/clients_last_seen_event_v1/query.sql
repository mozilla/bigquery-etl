{% if is_init() %}
  SELECT
    CAST(NULL AS INT64) AS days_logged_event_bits,
    CAST(NULL AS INT64) AS days_used_pictureinpicture_bits,
    CAST(NULL AS INT64) AS days_viewed_protection_report_bits,
  -- We make sure to delay * until the end so that as new columns are added
  -- to clients_daily_event we can add those columns in the same order to the end
  -- of this schema, which may be necessary for the daily join query between
  -- the two tables to validate.
    *
  FROM
    clients_daily_event_v1
  WHERE
  -- Output empty table and read no input rows
    FALSE
{% else %}
  WITH _current AS (
    SELECT
      CAST(n_logged_event > 0 AS INT64) AS days_logged_event_bits,
      CAST(n_created_pictureinpicture > 0 AS INT64) AS days_used_pictureinpicture_bits,
      CAST(n_viewed_protection_report > 0 AS INT64) AS days_viewed_protection_report_bits,
      * EXCEPT (submission_date)
    FROM
      clients_daily_event_v1
    WHERE
      submission_date = @submission_date
  ),
  _previous AS (
    SELECT
    -- We have to list out bit pattern fields explicitly here in exactly the
    -- order they appear in the _current CTE above. If there is any mismatch
    -- of field names, the IF().* statement in the final query will raise
    -- an error.
      days_logged_event_bits,
      days_used_pictureinpicture_bits,
      days_viewed_protection_report_bits,
      * EXCEPT (
        days_logged_event_bits,
        days_used_pictureinpicture_bits,
        days_viewed_protection_report_bits,
        submission_date
      )
    FROM
      clients_last_seen_event_v1
    WHERE
      submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
      AND udf.shift_28_bits_one_day(days_logged_event_bits) > 0
  )
  SELECT
    @submission_date AS submission_date,
    IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
      udf.combine_adjacent_days_28_bits(
        _previous.days_logged_event_bits,
        _current.days_logged_event_bits
      ) AS days_logged_event_bits,
      udf.combine_adjacent_days_28_bits(
        _previous.days_used_pictureinpicture_bits,
        _current.days_used_pictureinpicture_bits
      ) AS days_used_pictureinpicture_bits,
      udf.combine_adjacent_days_28_bits(
        _previous.days_viewed_protection_report_bits,
        _current.days_viewed_protection_report_bits
      ) AS days_viewed_protection_report_bits
    )
  FROM
    _current
  FULL JOIN
    _previous
    USING (client_id)
{% endif %}
