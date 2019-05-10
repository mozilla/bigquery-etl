-- Equivalent to, but more efficient than, calling udf_bitmask_range(1, 28)
CREATE TEMP FUNCTION bitmask_lowest_28() AS (0x0FFFFFFF);
--
CREATE TEMP FUNCTION shift_one_day(x INT64) AS (IFNULL((x << 1) & bitmask_lowest_28(), 0));
--
CREATE TEMP FUNCTION combine_days(prev INT64, curr INT64) AS (shift_one_day(prev) + IFNULL(curr, 0));
--
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
    DATE_DIFF(submission_date_s3, SAFE.PARSE_DATE("%F", SUBSTR(profile_creation_date, 0, 10)), DAY) AS days_since_created_profile,
    CAST(NULL AS BOOLEAN) AS ping_seen_within_6_days_of_profile_creation,
    * EXCEPT (submission_date_s3)
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date ),
  --
  _previous AS (
  SELECT
    * EXCEPT (submission_date)
    REPLACE (
    -- Scrub values outside 28 day window.
    IF(days_since_created_profile BETWEEN 0 AND 26, days_since_created_profile, NULL) AS days_since_created_profile)
  FROM
    clients_last_seen_raw_v1 AS cls
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND shift_one_day(days_seen_bits) > 0),
  --
  _joined AS (
  SELECT
    @submission_date AS submission_date,
    IF(_current.client_id IS NOT NULL,
      _current,
      _previous).* REPLACE (
        combine_days(_previous.days_seen_bits, _current.days_seen_bits) AS days_seen_bits,
        combine_days(_previous.days_visited_5_uri_bits, _current.days_visited_5_uri_bits) AS days_visited_5_uri_bits,
        combine_days(_previous.days_opened_dev_tools_bits, _current.days_opened_dev_tools_bits) AS days_opened_dev_tools_bits,
        -- We want to base new profile creation date on the first profile_creation_date
        -- value we observe, so we propagate an existing non-null value in preference
        -- to a non-null value on today's observation.
        COALESCE(_previous.days_since_created_profile + 1,
          _current.days_since_created_profile) AS days_since_created_profile,
        -- We only trust profile_creation_date if we see a ping within one week,
        -- so we calculate this on day 6 and propagate to subsequent days.
        IF(COALESCE(_previous.days_since_created_profile + 1,
        _current.days_since_created_profile) = 6, TRUE, _previous.ping_seen_within_6_days_of_profile_creation) AS ping_seen_within_6_days_of_profile_creation)
  FROM
    _current
  FULL JOIN
    _previous
  USING
    -- Include sample_id to match the clustering of the tables, which may improve
    -- join performance.
    (sample_id, client_id))
  --
SELECT
  * REPLACE (
    -- Null out any fields that may contain data leaked from beyond our 28 day window.
    IF(days_since_created_profile BETWEEN 0 AND 27, days_since_created_profile, NULL) AS days_since_created_profile,
    IF(days_since_created_profile BETWEEN 0 AND 27, ping_seen_within_6_days_of_profile_creation, NULL) AS ping_seen_within_6_days_of_profile_creation)
FROM
  _joined
