-- Query for telemetry health ping latency across all applications
{% for app in applications %}
(
  WITH sample AS (
    SELECT
      "{{ application_names[app] }}" AS application,
      normalized_channel AS channel,
      metadata.header.parsed_date,
      ping_info.parsed_end_time,
      submission_timestamp,
    FROM
      `{{ project_id }}.{{ app }}.baseline`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
  ),
  latency_quantiles AS (
    SELECT
      application,
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
      application,
      channel,
      submission_date
  )
  SELECT
    application,
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
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
