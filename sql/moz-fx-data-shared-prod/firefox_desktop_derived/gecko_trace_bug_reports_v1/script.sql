CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_bug_reports_v1`(
    submission_date DATE,
    stable_trace_id STRING,
    app_id STRING,
    bug_id INT64,
    filed_date DATE
  )
CLUSTER BY
  stable_trace_id,
  app_id;
