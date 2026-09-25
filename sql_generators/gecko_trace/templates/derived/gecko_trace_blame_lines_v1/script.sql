CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_blame_lines_v1`(
    tree STRING,
    git_rev STRING,
    source_file STRING,
    lineno INT64,
    origin_rev STRING,
    origin_path STRING,
    origin_lineno INT64,
    fetched_date DATE
  )
CLUSTER BY
  git_rev,
  source_file;
