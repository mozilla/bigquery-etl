CREATE TEMP FUNCTION bitmask_lowest_7() AS (0x7F);
CREATE TEMP FUNCTION bitcount_lowest_7(x INT64) AS (BIT_COUNT(x & bitmask_lowest_7()));
CREATE TEMP FUNCTION udf_active_in_week_1(x INT64) AS (bitcount_lowest_7(x) > 0);
CREATE TEMP FUNCTION udf_active_in_week_0(x INT64) AS (bitcount_lowest_7(x >> 7) > 0);
--
CREATE TEMP FUNCTION
  udf_bit_substr(x INT64,
    _position INT64,
    _length INT64) AS ((
    SELECT
      x & SUM(1 << (_n - 1))
    FROM
      UNNEST((
        SELECT
          GENERATE_ARRAY(_position, _position + _length - 1))) AS _n ));
--
CREATE
  OR REPLACE
TABLE
  telemetry.smoot_usage_2week_raw_v1
PARTITION BY
  date AS
WITH nested AS (
  SELECT
    DATE_SUB(submission_date, INTERVAL 13 DAY) AS date,
    COUNT(*) AS new_profiles,
    [ --
      STRUCT('Any Firefox Desktop Activity' AS usage,
        COUNTIF(udf_active_in_week_0(days_seen_bits)) AS active_in_week_0,
        COUNTIF(udf_active_in_week_1(days_seen_bits)) AS active_in_week_1,
        COUNTIF(udf_active_in_week_0(days_seen_bits) AND udf_active_in_week_1(days_seen_bits)) AS active_in_week_0_and_1),
      STRUCT('Firefox Desktop Visited 5 URI' AS usage,
        COUNTIF(udf_active_in_week_0(days_visited_5_uri_bits)) AS active_in_week_0,
        COUNTIF(udf_active_in_week_1(days_visited_5_uri_bits)) AS active_in_week_1,
        COUNTIF(udf_active_in_week_0(days_visited_5_uri_bits) AND udf_active_in_week_1(days_visited_5_uri_bits)) AS active_in_week_0_and_1),
      STRUCT('Firefox Desktop Dev Tools Opened' AS usage,
        COUNTIF(udf_active_in_week_0(days_opened_dev_tools_bits)) AS active_in_week_0,
        COUNTIF(udf_active_in_week_1(days_opened_dev_tools_bits)) AS active_in_week_1,
        COUNTIF(udf_active_in_week_0(days_opened_dev_tools_bits) AND udf_active_in_week_1(days_opened_dev_tools_bits)) AS active_in_week_0_and_1) --
    ] AS metrics,
    -- We hash client_ids into 20 buckets to aid in computing
    -- confidence intervals for mau/wau/dau sums; the particular hash
    -- function and number of buckets is subject to change in the future.
    MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
    -- requested fields from bug 1525689
    attribution.source,
    attribution.medium,
    attribution.campaign,
    attribution.content,
    country,
    distribution_id
  FROM
    telemetry.smoot_clients_last_seen_1percent_v1
  WHERE
    client_id IS NOT NULL
    AND days_since_created_profile = 13
    AND ping_seen_within_6_days_of_profile_creation
    --AND submission_date = @submission_date
  GROUP BY
    submission_date,
    id_bucket,
    source,
    medium,
    campaign,
    content,
    country,
    distribution_id
  )
--
SELECT
  *
FROM
  nested
WHERE
  ARRAY_LENGTH(metrics) > 0
