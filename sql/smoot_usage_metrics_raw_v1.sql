CREATE TEMP FUNCTION bitmask_lowest_7() AS (0x7F);
CREATE TEMP FUNCTION bitcount_lowest_7(x INT64) AS (BIT_COUNT(x & bitmask_lowest_7()));

CREATE
  OR REPLACE
TABLE
  telemetry.smoot_usage_metrics_raw_v1
PARTITION BY
  date AS
SELECT
  submission_date AS date,
  [
  STRUCT('Any Firefox Desktop Activity' AS usage, 'MAU' AS metric, COUNTIF(days_since_seen < 28) AS value),
  STRUCT('Any Firefox Desktop Activity', 'WAU', COUNTIF(days_since_seen < 7)),
  STRUCT('Any Firefox Desktop Activity', 'DAU', COUNTIF(days_since_seen < 1)),
  STRUCT('Any Firefox Desktop Activity', 'Intensity', SUM(bitcount_lowest_7(days_seen_bits))/COUNTIF(days_since_seen < 7)),
  STRUCT('Firefox Desktop Visited 5 URI', 'MAU', COUNTIF(days_since_visited_5_uri < 28)),
  STRUCT('Firefox Desktop Visited 5 URI', 'WAU', COUNTIF(days_since_visited_5_uri < 7)),
  STRUCT('Firefox Desktop Visited 5 URI', 'DAU', COUNTIF(days_since_visited_5_uri < 1)),
  --STRUCT('Firefox Desktop Visited 5 URI', 'Intensity', bitcount_lowest_7(days_seen_bits)/COUNTIF(days_since_seen < 7)),
  STRUCT('Firefox Desktop Dev Tools Opened', 'MAU', COUNTIF(days_since_opened_dev_tools < 28)),
  STRUCT('Firefox Desktop Dev Tools Opened', 'WAU', COUNTIF(days_since_opened_dev_tools < 7)),
  STRUCT('Firefox Desktop Dev Tools Opened', 'DAU', COUNTIF(days_since_opened_dev_tools < 1))
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
