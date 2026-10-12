WITH RECURSIVE {% include '_shared/trace_chain_ctes.sql' %},
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
)
SELECT
  @submission_date AS submission_date,
  trace_signature,
  COUNT(*) AS hit_count,
  AVG(CAST(duration_nano AS FLOAT64)) AS avg_duration_nano
FROM
  trace_instances
GROUP BY
  trace_signature
