-- Query for telemetry health sequence holes
WITH
  sample AS (
  SELECT
    {{ app_name }} AS application,
    normalized_channel AS channel,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    ping_info.seq AS sequence_number
  FROM
    `{{ baseline_table }}`
  WHERE
    sample_id = 0
    AND DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),
  lagged AS (
  SELECT
    application,
    channel,
    submission_date,
    client_id,
    sequence_number,
    LAG(sequence_number) OVER (PARTITION BY submission_date, client_id ORDER BY sequence_number ) AS prev_seq
  FROM
    sample ),
  per_client_day AS (
  SELECT
    application,
    channel,
    submission_date,
    client_id,
    -- A client has a gap on that date if any step isn't prev+1.
    LOGICAL_OR(prev_seq IS NOT NULL
      AND sequence_number != prev_seq + 1) AS has_gap
  FROM
    lagged
  GROUP BY
    ALL )
SELECT
  application,
  channel,
  submission_date,
  COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
  COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
  SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
FROM
  per_client_day
GROUP BY
  ALL
ORDER BY
  application,
  channel,
  submission_date
