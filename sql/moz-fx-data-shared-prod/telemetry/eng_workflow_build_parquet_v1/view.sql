CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.eng_workflow_build_parquet_v1`
AS
SELECT
  DATE(submission_timestamp) AS submission_date,
  DATE(submission_timestamp) AS submission_date_s3,
  STRUCT(
    document_id,
    UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`
  ) AS metadata,
  argv,
  build_opts,
  client_id,
  command,
  duration_ms,
  success,
  STRUCT(
    system.os,
    system.cpu_brand,
    system.drive_is_ssd,
    CAST(system.logical_cores AS int64) AS logical_cores,
    CAST(system.memory_gb AS int64) AS memory_gb,
    CAST(system.physical_cores AS int64) AS physical_cores,
    system.virtual_machine
  ) AS system,
  FORMAT_TIMESTAMP('%FT%X', time) AS time,
  exception,
  ARRAY(
    SELECT AS STRUCT
      u.* REPLACE (CAST(count AS int64) AS count)
    FROM
      UNNEST(file_types_changed) AS u
  ) AS file_types_changed,
  build_attrs
FROM
  `moz-fx-data-shared-prod.eng_workflow.build`
