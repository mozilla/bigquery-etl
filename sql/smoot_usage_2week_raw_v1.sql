CREATE TEMP FUNCTION bitmask_lowest_7() AS (0x7F);
CREATE TEMP FUNCTION bitcount_lowest_7(x INT64) AS (BIT_COUNT(x & bitmask_lowest_7()));
CREATE TEMP FUNCTION udf_active_in_week_1(x INT64) AS (bitcount_lowest_7(x >> 7) > 0);

CREATE
  OR REPLACE
TABLE
  telemetry.smoot_usage_2week_raw_v1
PARTITION BY
  date AS
WITH nested AS (
  SELECT
    DATE_SUB(submission_date, INTERVAL 14 DAY) AS date,
    [ --
      STRUCT('Any Firefox Desktop Activity' AS usage,
        COUNTIF(udf_active_in_week_1(days_seen_bits)) AS retained_in_week_1),
      STRUCT('Firefox Desktop Visited 5 URI' AS usage,
        COUNTIF(udf_active_in_week_1(days_visited_5_uri_bits)) AS retained_in_week_1),
      STRUCT('Firefox Desktop Dev Tools Opened' AS usage,
        COUNTIF(udf_active_in_week_1(days_opened_dev_tools_bits)) AS retained_in_week_1) --
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
    AND days_since_created_profile = 14
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
  ),
--
nonzero AS (
  SELECT
    * EXCEPT (metrics),
    ARRAY(SELECT AS STRUCT * FROM UNNEST(metrics) WHERE retained_in_week_1 > 0) AS metrics
  FROM
    nested
)
--
SELECT
  *
FROM
  nonzero
WHERE
  ARRAY_LENGTH(metrics) > 0
