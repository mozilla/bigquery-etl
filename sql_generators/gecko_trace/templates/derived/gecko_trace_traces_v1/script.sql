CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1`(
    submission_date DATE,
    stable_trace_id STRING,
    trace_signature STRING,
    hit_count INT64,
    avg_duration_nano FLOAT64
  )
CLUSTER BY
  trace_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` AS T
  USING (
    WITH RECURSIVE {% include '_shared/trace_chain_ctes.sql' %}
    trace_instances AS (
      SELECT
        ts.trace_id,
        ts.trace_signature,
        MAX(rs.end_time_unix_nano) - MIN(rs.start_time_unix_nano) AS duration_nano
      FROM
        trace_signatures ts
      JOIN
        raw_spans rs
        ON ts.trace_id = rs.trace_id
      GROUP BY
        ts.trace_id,
        ts.trace_signature
    ),
    trace_aggregates AS (
      SELECT
        @submission_date AS submission_date,
        trace_signature,
        COUNT(*) AS hit_count,
        AVG(CAST(duration_nano AS FLOAT64)) AS avg_duration_nano
      FROM
        trace_instances
      GROUP BY
        trace_signature
    )
    SELECT
      *
    FROM
      trace_aggregates
  ) AS S
  ON T.trace_signature = S.trace_signature
  AND T.submission_date = S.submission_date
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (submission_date, stable_trace_id, trace_signature, hit_count, avg_duration_nano)
  VALUES
    (S.submission_date, GENERATE_UUID(), S.trace_signature, S.hit_count, S.avg_duration_nano)
  WHEN MATCHED
THEN
  UPDATE
  SET
    hit_count = S.hit_count,
    avg_duration_nano = S.avg_duration_nano;