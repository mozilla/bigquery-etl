WITH RECURSIVE {% include '_shared/trace_chain_ctes.sql' %},
trace_platforms AS (
  SELECT
    ts.trace_id,
    ts.trace_signature,
    ANY_VALUE(rs.app_build) AS app_build,
    ANY_VALUE(rs.normalized_os) AS normalized_os,
    ANY_VALUE(rs.normalized_os_version) AS normalized_os_version,
    ANY_VALUE(rs.architecture) AS architecture
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
  app_build,
  normalized_os,
  normalized_os_version,
  architecture,
  COUNT(*) AS hit_count
FROM
  trace_platforms
GROUP BY
  trace_signature,
  app_build,
  normalized_os,
  normalized_os_version,
  architecture
