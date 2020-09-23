CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.analysis.fission_monitoring_crash_v1`( -- TODO: s/analysis/telemetry_derived/
    submission_date DATE,
    experiment_branch STRING,
    build_id STRING,
    count INT64,
    main_crashes INT64,
    content_crashes INT64,
    startup_crashes INT64,
    content_shutdown_crashes INT64,
    gpu_crashes INT64,
    plugin_crashes INT64,
    gmplugin_crashes INT64,
    usage_hours FLOAT64
  )
PARTITION BY
  submission_date
