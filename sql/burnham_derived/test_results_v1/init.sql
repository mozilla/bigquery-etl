CREATE OR REPLACE TABLE
  burnham_derived.test_results_v1(
    submission_timestamp TIMESTAMP,
    test_run STRING,
    test_name STRING,
    test_outcome STRING,
    test_duration_millis INT64,
    test_log_url STRING,
    test_report STRING
  )
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  test_name
OPTIONS
  (
    description = "Result reports from running burnham; see https://github.com/mozilla/burnham/issues/39"
  )
