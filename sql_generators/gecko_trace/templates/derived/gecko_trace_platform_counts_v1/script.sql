CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_platform_counts_v1` (
    submission_date DATE,
    stable_trace_id STRING,
    trace_signature STRING,
    app_build STRING,
    normalized_os STRING,
    normalized_os_version STRING,
    architecture STRING,
    hit_count INT64
  )
  CLUSTER BY trace_signature;

MERGE INTO
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_platform_counts_v1` AS T
  USING (
    WITH RECURSIVE
{% include '_shared/trace_chain_ctes.sql' %}
platform_counts AS (
  SELECT
    @submission_date AS submission_date,
    tr.stable_trace_id,
    ts.trace_signature,
    rs.app_build,
    rs.normalized_os,
    rs.normalized_os_version,
    rs.architecture,
    COUNT(DISTINCT ts.trace_id) AS hit_count
  FROM
    trace_signatures ts
  JOIN
    raw_spans rs ON ts.trace_id = rs.trace_id
  JOIN
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` tr
    ON ts.trace_signature = tr.trace_signature
    AND tr.submission_date = @submission_date
  GROUP BY
    tr.stable_trace_id,
    ts.trace_signature,
    rs.app_build,
    rs.normalized_os,
    rs.normalized_os_version,
    rs.architecture
)
    SELECT
      *
    FROM
      platform_counts
  ) AS S
  ON T.stable_trace_id = S.stable_trace_id
  AND T.submission_date = S.submission_date
  AND T.app_build = S.app_build
  AND T.normalized_os = S.normalized_os
  AND T.normalized_os_version = S.normalized_os_version
  AND T.architecture = S.architecture
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    submission_date,
    stable_trace_id,
    trace_signature,
    app_build,
    normalized_os,
    normalized_os_version,
    architecture,
    hit_count
  )
  VALUES (
    S.submission_date,
    S.stable_trace_id,
    S.trace_signature,
    S.app_build,
    S.normalized_os,
    S.normalized_os_version,
    S.architecture,
    S.hit_count
  )
WHEN MATCHED THEN
  UPDATE SET
    trace_signature = S.trace_signature,
    hit_count = S.hit_count;
