DECLARE submission_date DATE DEFAULT CURRENT_DATE() - 1;

SET submission_date = @submission_date;

WITH
{% include '_shared/trace_chain_ctes.sql' %}
-- Aggregate trace instances for timestamps and duration
trace_instances AS (
  SELECT
    ts.trace_id,
    ts.trace_signature,
    MIN(rs.submission_timestamp) AS first_submission_timestamp,
    MAX(rs.submission_timestamp) AS latest_submission_timestamp,
    MAX(rs.end_time_unix_nano) - MIN(rs.start_time_unix_nano) AS duration_nano
  FROM
    trace_signature_cte ts
  JOIN
    raw_spans rs ON ts.trace_id = rs.trace_id
  GROUP BY
    ts.trace_id,
    ts.trace_signature
),
-- Aggregate by trace signature for hit counts and dates
trace_aggregates AS (
  SELECT
    @submission_date AS submission_date,
    trace_signature,
    COUNT(*) AS hit_count,
    MIN(DATE(first_submission_timestamp)) AS first_seen_date,
    MAX(DATE(latest_submission_timestamp)) AS latest_seen_date,
    AVG(CAST(duration_nano AS FLOAT64)) AS avg_duration_nano
  FROM
    trace_instances
  GROUP BY
    trace_signature
)
-- MERGE into target table
MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` AS T
USING
  trace_aggregates AS S
ON
  T.trace_signature = S.trace_signature
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_trace_id,
    trace_signature,
    hit_count,
    first_seen_date,
    latest_seen_date,
    avg_duration_nano
  )
  VALUES (
    S.submission_date,
    GENERATE_UUID(),
    S.trace_signature,
    S.hit_count,
    S.first_seen_date,
    S.latest_seen_date,
    S.avg_duration_nano
  )
WHEN MATCHED THEN
  UPDATE SET
    hit_count = T.hit_count + S.hit_count,
    latest_seen_date = GREATEST(T.latest_seen_date, S.latest_seen_date),
    avg_duration_nano = (T.avg_duration_nano * T.hit_count + S.avg_duration_nano * S.hit_count) / (T.hit_count + S.hit_count);
