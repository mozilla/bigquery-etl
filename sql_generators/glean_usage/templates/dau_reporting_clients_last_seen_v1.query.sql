{{ header }}

WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit in 'days_since_seen' represents whether the user sent a
    -- dau_reporting ping in the submission_date and similarly, the rightmost bit in
    -- days_active_bits represents whether the user counts as active on that date.
    CAST(TRUE AS INT64) AS days_seen_bits,
    {% if app_name == "firefox_desktop" %}
    -- 0 uris and > 0 active ticks on desktop
    CAST(TRUE AS INT64) & CAST(browser_engagement_uri_count > 0  AS INT64) & CAST(browser_engagement_active_ticks > 0  AS INT64) AS days_active_bits,
    {% else %}
    CAST(TRUE AS INT64) & CAST(durations > 0  AS INT64) AS days_active_bits,
    {% endif %}
    udf.days_since_created_profile_as_28_bits(
      DATE_DIFF(submission_date, first_run_date, DAY)
    ) AS days_created_profile_bits,
    client_id,
    usage_profile_id,
  FROM
    `{{ dau_reporting_clients_daily_table }}`
  WHERE
    submission_date = @submission_date
),
_previous AS (
  SELECT
    days_seen_bits,
    days_active_bits,
    days_created_profile_bits,
    client_id,
    usage_profile_id
  FROM
    `{{ dau_reporting_clients_last_seen_table }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_active_bits,
      _current.days_active_bits
    ) AS days_active_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_created_profile_bits,
      _current.days_created_profile_bits
    ) AS days_created_profile_bits
  )
FROM
  _current
FULL JOIN
  _previous
  USING (client_id, usage_profile_id)