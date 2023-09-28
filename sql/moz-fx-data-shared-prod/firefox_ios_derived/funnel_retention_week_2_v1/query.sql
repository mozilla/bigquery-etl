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
    -- this guard needs to be added if using the clients table
    -- alternatively it needs to be used inside the first_seen CTE
    -- AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)
    AND normalized_channel = "release"
),
first_seen AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
  FROM
    firefox_ios.baseline_clients_first_seen
  WHERE
    -- Two weeks need to elapse before calculating the week 2 retention
    submission_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)
    -- AND first_seen_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)
    AND normalized_channel = "release"
    AND NOT (app_display_version = '107.2' AND submission_date >= '2023-02-01')
  -- Alternative source table
  -- FROM
  --   firefox_ios.firefox_ios_clients
  -- WHERE
  --   channel = "release"
),
retention_calculation AS (
  SELECT
    first_seen.first_seen_date,
    clients_retention.client_id,
    clients_retention.sample_id,
    mozfun.bits28.retention(days_seen_bits, @submission_date) AS retention,
  FROM
    clients_retention
  INNER JOIN
    first_seen
  USING
    (client_id, sample_id)
)
SELECT
  * EXCEPT (retention) REPLACE(
  -- metric date should align with first_seen_date, if that is not the case then the query will fail.
    IF(
      retention.day_13.metric_date <> first_seen_date,
      ERROR("Metric date misaligned with first_seen_date"),
      first_seen_date
    ) AS first_seen_date
  ),
  -- retention UDF works on 0 index basis, that's why for example week_1 is aliased as week 2 to make it a bit more user friendly.
  retention.day_13.active_in_week_1 AS retained_week_2,
FROM
  retention_calculation
