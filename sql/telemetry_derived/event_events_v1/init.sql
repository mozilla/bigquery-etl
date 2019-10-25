CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.event_events_v1` (
    submission_date DATE,
    document_id STRING,
    client_id STRING,
    normalized_channel STRING,
    country STRING,
    locale STRING,
    app_name STRING,
    app_version STRING,
    os STRING,
    os_version STRING,
    experiments ARRAY < STRUCT < key STRING,
    value STRUCT < branch STRING,
    enrollment_id STRING,
    type STRING >> >,
    sample_id INT64,
    session_id STRING,
    session_start_time TIMESTAMP,
    subsession_id STRING,
    `timestamp` TIMESTAMP,
    event_timestamp INT64,
    event_category STRING,
    event_method STRING,
    event_object STRING,
    event_string_value STRING,
    event_map_values ARRAY < STRUCT < key STRING,
    value STRING >>,
    event_process STRING
  )
PARTITION BY
  submission_date CLUSTER BY event_category,
  sample_id OPTIONS (require_partition_filter = TRUE)
