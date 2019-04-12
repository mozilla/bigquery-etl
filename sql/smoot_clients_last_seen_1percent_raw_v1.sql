CREATE TEMP FUNCTION bitmask_lowest_28() AS (0x0FFFFFFF);
CREATE TEMP FUNCTION shift_one_day(x INT64) AS (IFNULL((x << 1) & bitmask_lowest_28(), 0));

WITH
  _current AS (
  SELECT
    * EXCEPT (submission_date_s3),
    CAST(TRUE AS INT64) AS days_seen_bits,
    -- For measuring Active MAU, where this is the days since this
    -- client_id was an Active User as defined by
    -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
    CAST(scalar_parent_browser_engagement_total_uri_count_sum >= 5 AS INT64) AS days_visited_5_uri_bits,
    CAST(devtools_toolbox_opened_count_sum > 0 AS INT64) AS days_opened_dev_tools_bits,
    profile_age_in_days AS days_since_created_profile
  FROM
    telemetry.smoot_clients_daily_1percent_v1
  WHERE
    submission_date_s3 = @submission_date ),
  --
  _previous AS (
  SELECT
    * EXCEPT (submission_date, generated_time)
  FROM
    telemetry.smoot_clients_last_seen_1percent_raw_v1 AS cls
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND shift_one_day(days_seen_bits) > 0)
--
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL,
    _current,
    _previous).* REPLACE (
      shift_one_day(_previous.days_seen_bits) + _current.days_seen_bits AS days_seen_bits,
      shift_one_day(_previous.days_visited_5_uri_bits) + _current.days_visited_5_uri_bits AS days_visited_5_uri_bits,
      shift_one_day(_previous.days_opened_dev_tools_bits) + _current.days_opened_dev_tools_bits AS days_opened_dev_tools_bits,
      COALESCE(_current.days_since_created_profile,
        _previous.days_since_created_profile + 1) AS days_since_created_profile)
FROM
  _current
FULL JOIN
  _previous
USING
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
  (sample_id, client_id)
