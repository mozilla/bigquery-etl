CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.fission_monitoring_crash_v1`(
    submission_date DATE,
    client_id STRING,
    experiment_branch STRING,
    build_id STRING,
    os_name STRING,
    os_version STRING,
    count INT64,
    main_crashes INT64,
    content_crashes INT64,
    startup_crashes INT64,
    content_shutdown_crashes INT64,
    oom_crashes INT64,
    shutdown_kill_crashes INT64,
    shutdown_hangs INT64,
    gpu_crashes INT64,
    plugin_crashes INT64,
    gmplugin_crashes INT64,
    usage_hours FLOAT64
  )
PARTITION BY
  submission_date
