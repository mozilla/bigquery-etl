WITH clients_retention AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    days_seen_bits,
  FROM
    firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND normalized_channel = "release"
),
first_seen AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
  FROM
    firefox_ios.baseline_clients_first_seen
  -- 28 days need to elapse before calculating the week 4 and day 28 retention metrics
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 28 DAY)
)
SELECT
  first_seen.first_seen_date,
  clients_retention.client_id,
  clients_retention.sample_id,
  mozfun.bits28.active_in_range(days_seen_bits, -6, 7) AS retained_week_4,
  LENGTH(REPLACE(mozfun.bits28.to_string(days_seen_bits), "0", "")) AS days_seen_in_first_28_days,
  -- TODO: Should we be using the UDF here?
  -- mozfun.bits28.retention(days_seen_bits, @submission_date).day_27.active_in_week_3 AS retention_week_4,
  -- mozfun.bits28.retention(days_seen_bits, @submission_date) AS retention, -- retention week 2 query should return the same result as day_13.active_in_week_1? Maybe a good test to make sure we're aligned with the UDF?
FROM
  clients_retention
INNER JOIN
  first_seen
USING
  (client_id, sample_id)
