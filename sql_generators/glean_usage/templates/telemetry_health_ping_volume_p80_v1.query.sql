-- Query for telemetry health ping volume p80
WITH
  sample AS (
  SELECT
    {{ app_name }} AS application,
    normalized_channel AS channel,
    DATE(submission_timestamp) AS submission_date,
    COUNT(1) AS ping_count
  FROM
    `{{ baseline_table }}`
  WHERE
    sample_id = 0
    AND DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY
    application,
    channel,
    submission_date,
    client_info.client_id ),
  ping_count_quantiles AS (
  SELECT
    application,
    channel,
    submission_date,
    APPROX_QUANTILES(ping_count, 100) AS quantiles,
  FROM
    sample
  GROUP BY
    ALL )
SELECT
  application,
  channel,
  submission_date,
  quantiles[OFFSET(80)] AS p80
FROM
  ping_count_quantiles
ORDER BY
  application,
  channel,
  submission_date