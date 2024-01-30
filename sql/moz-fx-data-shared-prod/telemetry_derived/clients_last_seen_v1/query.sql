-- Note that this query runs in the telemetry_derived dataset, so sees derived tables
-- rather than the user-facing views (so key_value structs haven't been eliminated, etc.)
WITH _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- For measuring Active MAU, where this is the days since this
    -- client_id was an Active User as defined by
    -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
    CAST(
      COALESCE(
        scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
        scalar_parent_browser_engagement_total_uri_count_sum
      ) >= 1 AS INT64
    ) AS days_visited_1_uri_bits,
    CAST(
      COALESCE(
        scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
        scalar_parent_browser_engagement_total_uri_count_sum
      ) >= 5 AS INT64
    ) AS days_visited_5_uri_bits,
    CAST(
      COALESCE(
        scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
        scalar_parent_browser_engagement_total_uri_count_sum
      ) >= 10 AS INT64
    ) AS days_visited_10_uri_bits,
    CAST(active_hours_sum >= 0.011 AS INT64) AS days_had_8_active_ticks_bits,
    CAST(devtools_toolbox_opened_count_sum > 0 AS INT64) AS days_opened_dev_tools_bits,
    CAST(active_hours_sum > 0 AS INT64) AS days_interacted_bits,
    CAST(
      scalar_parent_browser_engagement_total_uri_count_sum >= 1 AS INT64
    ) AS days_visited_1_uri_normal_mode_bits,
    -- This field is only available after version 84, see the definition in clients_daily_v6 view
    CAST(
      IF(
        mozfun.norm.extract_version(app_display_version, 'major') < 84,
        NULL,
        scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum - COALESCE(
          scalar_parent_browser_engagement_total_uri_count_sum,
          0
        )
      ) >= 1 AS INT64
    ) AS days_visited_1_uri_private_mode_bits,
    -- We only trust profile_date if it is within one week of the ping submission,
    -- so we ignore any value more than seven days old.
    udf.days_since_created_profile_as_28_bits(
      DATE_DIFF(submission_date, SAFE.PARSE_DATE("%F", SUBSTR(profile_creation_date, 0, 10)), DAY)
    ) AS days_created_profile_bits,
    -- Experiments are an array, so we keep track of a usage bit pattern per experiment.
    ARRAY(
      SELECT AS STRUCT
        key AS experiment,
        value AS branch,
        1 AS bits
      FROM
        UNNEST(experiments)
    ) AS days_seen_in_experiment,
    * EXCEPT (submission_date)
  FROM
    clients_daily_v6
  WHERE
    submission_date = @submission_date
),
--
_previous AS (
  SELECT
    days_seen_bits,
    days_visited_1_uri_bits,
    days_visited_5_uri_bits,
    days_visited_10_uri_bits,
    days_had_8_active_ticks_bits,
    days_opened_dev_tools_bits,
    days_interacted_bits,
    days_visited_1_uri_normal_mode_bits,
    days_visited_1_uri_private_mode_bits,
    days_created_profile_bits,
    days_seen_in_experiment,
    * EXCEPT (
      days_seen_bits,
      days_visited_1_uri_bits,
      days_visited_5_uri_bits,
      days_visited_10_uri_bits,
      days_had_8_active_ticks_bits,
      days_opened_dev_tools_bits,
      days_interacted_bits,
      days_visited_1_uri_normal_mode_bits,
      days_visited_1_uri_private_mode_bits,
      days_created_profile_bits,
      days_seen_in_experiment,
      submission_date,
      first_seen_date,
      second_seen_date
    )
  FROM
    clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
--
SELECT
  @submission_date AS submission_date,
  IF(cfs.first_seen_date > @submission_date, NULL, cfs.first_seen_date) AS first_seen_date,
  IF(cfs.second_seen_date > @submission_date, NULL, cfs.second_seen_date) AS second_seen_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_visited_1_uri_bits,
      _current.days_visited_1_uri_bits
    ) AS days_visited_1_uri_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_visited_5_uri_bits,
      _current.days_visited_5_uri_bits
    ) AS days_visited_5_uri_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_visited_10_uri_bits,
      _current.days_visited_10_uri_bits
    ) AS days_visited_10_uri_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_had_8_active_ticks_bits,
      _current.days_had_8_active_ticks_bits
    ) AS days_had_8_active_ticks_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_opened_dev_tools_bits,
      _current.days_opened_dev_tools_bits
    ) AS days_opened_dev_tools_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_interacted_bits,
      _current.days_interacted_bits
    ) AS days_interacted_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_visited_1_uri_normal_mode_bits,
      _current.days_visited_1_uri_normal_mode_bits
    ) AS days_visited_1_uri_normal_mode_bits,
    udf.combine_adjacent_days_28_bits(
      _previous.days_visited_1_uri_private_mode_bits,
      _current.days_visited_1_uri_private_mode_bits
    ) AS days_visited_1_uri_private_mode_bits,
    udf.coalesce_adjacent_days_28_bits(
      _previous.days_created_profile_bits,
      _current.days_created_profile_bits
    ) AS days_created_profile_bits,
    udf.combine_experiment_days(
      _previous.days_seen_in_experiment,
      _current.days_seen_in_experiment
    ) AS days_seen_in_experiment
  )
FROM
  _current
FULL JOIN
  _previous
  USING (client_id)
LEFT JOIN
  clients_first_seen_v2 AS cfs
  USING (client_id)
