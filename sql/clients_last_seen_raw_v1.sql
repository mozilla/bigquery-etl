  -- Equivalent to, but more efficient than, calling udf_bitmask_range(1, 28)
WITH
  _current AS (
  SELECT
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- For measuring Active MAU, where this is the days since this
    -- client_id was an Active User as defined by
    -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
    CAST(scalar_parent_browser_engagement_total_uri_count_sum >= 5 AS INT64) AS days_visited_5_uri_bits,
    CAST(devtools_toolbox_opened_count_sum > 0 AS INT64) AS days_opened_dev_tools_bits,
    -- We only trust profile_date if it is within one week of the ping submission,
    -- so we ignore any value more than seven days old.
    udf_bits_from_days_since_created_profile(
      DATE_DIFF(submission_date_s3, SAFE.PARSE_DATE("%F",
        SUBSTR(profile_creation_date, 0, 10)), DAY)) AS days_created_profile_bits,
    -- Experiments are an array, so we keep track of a usage bit pattern per experiment.
    ARRAY(
    SELECT
      AS STRUCT --
      key AS experiment,
      value AS branch,
      1 AS bits
    FROM
      UNNEST(experiments.key_value)) AS days_seen_in_experiment,
    * EXCEPT (submission_date_s3)
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date ),
  --
  _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    clients_last_seen_raw_v1 AS cls
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
    _previous).* REPLACE (
      udf_combine_adjacent_days_bits(_previous.days_seen_bits,
        _current.days_seen_bits) AS days_seen_bits,
      udf_combine_adjacent_days_bits(_previous.days_visited_5_uri_bits,
        _current.days_visited_5_uri_bits) AS days_visited_5_uri_bits,
      udf_combine_adjacent_days_bits(_previous.days_opened_dev_tools_bits,
        _current.days_opened_dev_tools_bits) AS days_opened_dev_tools_bits,
      udf_coalesce_adjacent_days_bits(_previous.days_created_profile_bits,
        _current.days_created_profile_bits) AS days_created_profile_bits,
      udf_combine_experiment_days(_previous.days_seen_in_experiment,
        _current.days_seen_in_experiment) AS days_seen_in_experiment)
FROM
  _current
FULL JOIN
  _previous
USING
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
  (sample_id,
    client_id)
