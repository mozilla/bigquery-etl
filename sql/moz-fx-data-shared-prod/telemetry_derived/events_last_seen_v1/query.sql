WITH _current AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_logged_event_bits,
    CAST(
      LOGICAL_OR(event_method = 'show' AND event_object = 'protection_report') AS INT64
    ) AS days_viewed_protection_report_bits,
    CAST(
      LOGICAL_OR(event_category = 'pictureinpicture' AND event_method = 'create') AS INT64
    ) AS days_used_picture_in_picture_bits,
    CAST(
      LOGICAL_OR(event_string_value = 'SEC_ERROR_UNKNOWN_ISSUER') AS INT64
    ) AS days_had_cert_error_bits,
  FROM
    telemetry.events
  WHERE
    submission_date = @submission_date
),
--
_previous AS (
  SELECT
    *
  FROM
    events_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_logged_events_bits) > 0
)
--
SELECT
  @submission_date AS submission_date,
  udf.combine_adjacent_days_28_bits(
    _previous.days_logged_event_bits,
    _current.days_logged_event_bits
  ) AS days_logged_event_bits,
  udf.combine_adjacent_days_28_bits(
    _previous.days_viewed_protection_report_bits,
    _current.days_viewed_protection_report_bits
  ) AS days_viewed_protection_report_bits,
  udf.combine_adjacent_days_28_bits(
    _previous.days_used_picture_in_picture_bits,
    _current.days_used_picture_in_picture_bits
  ) AS days_used_picture_in_picture_bits,
  udf.combine_adjacent_days_28_bits(
    _previous.days_had_cert_error_bits,
    _current.days_had_cert_error_bits
  ) AS days_had_cert_error_bits,
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
