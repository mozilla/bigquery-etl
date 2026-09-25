CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_bug_reports_v1`(
    stable_trace_id STRING,
    bug_id INT64,
    filed_date DATE,
    last_comment_date DATE,
    last_reported_trace_signature STRING
  )
CLUSTER BY
  stable_trace_id;
