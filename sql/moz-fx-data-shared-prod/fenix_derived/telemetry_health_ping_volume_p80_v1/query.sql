-- Query for telemetry health ping volume p80
WITH sample AS (
  SELECT
    normalized_channel AS channel,
    DATE(submission_timestamp) AS submission_date,
    COUNT(1) AS ping_count
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    sample_id = 0
    AND DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY
    channel,
    submission_date,
    client_info.client_id
),
ping_count_quantiles AS (
  SELECT
    channel,
    submission_date,
    APPROX_QUANTILES(ping_count, 100) AS quantiles,
  FROM
    sample
  GROUP BY
    ALL
)
SELECT
  channel,
  submission_date,
  quantiles[OFFSET(80)] AS p80
FROM
  ping_count_quantiles
