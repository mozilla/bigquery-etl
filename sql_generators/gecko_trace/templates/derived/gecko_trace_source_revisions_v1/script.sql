CREATE TABLE IF NOT EXISTS
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_source_revisions_v1`(
    app_build STRING,
    channel STRING,
    tree STRING,
    hg_rev STRING,
    git_rev STRING,
    resolved_date DATE
  )
CLUSTER BY
  app_build;
