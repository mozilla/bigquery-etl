WITH sample AS (
  SELECT
    normalized_channel AS channel,
    metadata.header.parsed_date,
    ping_info.parsed_end_time,
    submission_timestamp,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline`
  WHERE
    sample_id = 0
    AND DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
latency_quantiles AS (
  SELECT
    channel,
    DATE(submission_timestamp) AS submission_date,
    APPROX_QUANTILES(
      TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
      100
    ) AS collection_to_submission_latency,
    APPROX_QUANTILES(
      TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
      100
    ) AS submission_to_ingestion_latency,
    APPROX_QUANTILES(
      TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
      100
    ) AS collection_to_ingestion_latency
  FROM
    sample
  GROUP BY
    ALL
)
SELECT
  channel,
  submission_date,
  collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
  collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
  submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
  submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
  collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
  collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
FROM
  latency_quantiles
