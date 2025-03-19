-- Generated via `usage_reporting` SQL generator.
-- In this raw table, we capture the history of activity over the past
-- 28 days for each usage criterion as a single 64-bit integer.
WITH _current AS (
  SELECT
    usage_profile_id,
    app_channel,
    -- The rightmost bit in 'days_since_seen' represents whether the user sent a usage_reporting ping on the submission_date.
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- The rightmost bit in days_active_bits represents whether the user counts as active on the submission_date.
    CAST(TRUE AS INT64) & CAST(is_active AS INT64) AS days_active_bits,
    `moz-fx-data-shared-prod.udf.days_since_created_profile_as_28_bits`(
      DATE_DIFF(submission_date, first_run_date, DAY)
    ) AS days_created_profile_bits,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly.usage_reporting_clients_daily`
  WHERE
    submission_date = @submission_date
),
_previous AS (
  SELECT
    usage_profile_id,
    app_channel,
    days_seen_bits,
    days_active_bits,
    days_created_profile_bits,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly.usage_reporting_clients_last_seen`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND `moz-fx-data-shared-prod.udf.shift_28_bits_one_day`(days_seen_bits) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.usage_profile_id IS NOT NULL, _current, _previous).* REPLACE (
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
      _previous.days_active_bits,
      _current.days_active_bits
    ) AS days_active_bits,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_28_bits`(
      _previous.days_created_profile_bits,
      _current.days_created_profile_bits
    ) AS days_created_profile_bits
  )
FROM
  _current
FULL JOIN
  _previous
  USING (usage_profile_id)
