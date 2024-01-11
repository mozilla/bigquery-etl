WITH jobs_by_org AS (
  SELECT
    jobs.project_id AS source_project,
    creation_date,
    job_id,
    job_type,
    reservation_id,
    cache_hit,
    state,
    statement_type,
    referenced_table.project_id AS reference_project_id,
    referenced_table.dataset_id AS reference_dataset_id,
    referenced_table.table_id AS reference_table_id,
    destination_table.project_id AS destination_project_id,
    destination_table.dataset_id AS destination_dataset_id,
    destination_table.table_id AS destination_table_id,
    user_email,
    end_time - start_time AS task_duration,
    ROUND(total_bytes_processed / 1024 / 1024 / 1024 / 1024, 4) AS total_terabytes_processed,
    ROUND(total_bytes_billed / 1024 / 1024 / 1024 / 1024, 4) AS total_terabytes_billed,
    total_slot_ms,
    error_result.location AS error_location,
    error_result.reason AS error_reason,
    error_result.message AS error_message,
    query_info_resource_warning AS resource_warning,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS jobs
  LEFT JOIN
    UNNEST(referenced_tables) AS referenced_table
),
jobs_by_project AS (
  SELECT
    jp.project_id AS source_project,
    DATE(creation_time) AS creation_date,
    job_id,
    referenced_table.project_id AS reference_project_id,
    referenced_table.dataset_id AS reference_dataset_id,
    referenced_table.table_id AS reference_table_id,
    user_email,
    REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
    REGEXP_EXTRACT(query, r'Query ID: (\w+), ') AS query_id,
  FROM
    `mozdata.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS jp
  LEFT JOIN
    UNNEST(
      ARRAY_CONCAT(
        referenced_tables,
        (
          SELECT
            ARRAY_AGG(
              STRUCT(
                materialized_view.table_reference.project_id AS project_id,
                materialized_view.table_reference.dataset_id AS dataset_id,
                materialized_view.table_reference.table_id AS table_id
              )
            )
          FROM
            UNNEST(materialized_view_statistics.materialized_view) AS materialized_view
        )
      )
    ) AS referenced_table
  WHERE
    DATE(creation_time) = @submission_date
  UNION ALL
  SELECT
    jp.project_id AS source_project,
    DATE(creation_time) AS creation_date,
    job_id,
    referenced_table.project_id AS reference_project_id,
    referenced_table.dataset_id AS reference_dataset_id,
    referenced_table.table_id AS reference_table_id,
    user_email,
    REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
    REGEXP_EXTRACT(query, r'Query ID: (\w+), ') AS query_id,
  FROM
    `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS jp
  LEFT JOIN
    UNNEST(
      ARRAY_CONCAT(
        referenced_tables,
        (
          SELECT
            ARRAY_AGG(
              STRUCT(
                materialized_view.table_reference.project_id AS project_id,
                materialized_view.table_reference.dataset_id AS dataset_id,
                materialized_view.table_reference.table_id AS table_id
              )
            )
          FROM
            UNNEST(materialized_view_statistics.materialized_view) AS materialized_view
        )
      )
    ) AS referenced_table
  WHERE
    DATE(creation_time) = @submission_date
  UNION ALL
  SELECT
    jp.project_id AS source_project,
    DATE(creation_time) AS creation_date,
    job_id,
    referenced_table.project_id AS reference_project_id,
    referenced_table.dataset_id AS reference_dataset_id,
    referenced_table.table_id AS reference_table_id,
    user_email,
    REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
    REGEXP_EXTRACT(query, r'Query ID: (\w+), ') AS query_id,
  FROM
    `moz-fx-data-marketing-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS jp
  LEFT JOIN
    UNNEST(
      ARRAY_CONCAT(
        referenced_tables,
        (
          SELECT
            ARRAY_AGG(
              STRUCT(
                materialized_view.table_reference.project_id AS project_id,
                materialized_view.table_reference.dataset_id AS dataset_id,
                materialized_view.table_reference.table_id AS table_id
              )
            )
          FROM
            UNNEST(materialized_view_statistics.materialized_view) AS materialized_view
        )
      )
    ) AS referenced_table
  WHERE
    DATE(creation_time) = @submission_date
)
SELECT DISTINCT
  jo.source_project,
  jo.creation_date,
  jo.job_id,
  jo.job_type,
  jo.reservation_id,
  jo.cache_hit,
  jo.state,
  jo.statement_type,
  jp.query_id,
  jo.reference_project_id,
  jo.reference_dataset_id,
  jo.reference_table_id,
  jo.destination_project_id,
  jo.destination_dataset_id,
  jo.destination_table_id,
  jo.user_email,
  jp.username,
  jo.task_duration,
  jo.total_terabytes_processed,
  jo.total_terabytes_billed,
  jo.total_slot_ms,
  jo.error_location,
  jo.error_reason,
  jo.error_message,
  jo.resource_warning,
  @submission_date AS submission_date,
FROM
  jobs_by_org AS jo
LEFT JOIN
  jobs_by_project AS jp
USING
  (
    source_project,
    creation_date,
    job_id,
    reference_project_id,
    reference_dataset_id,
    reference_table_id
  )
WHERE
  creation_date = @submission_date
