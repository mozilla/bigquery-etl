{% set DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
] %}
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
    bi_engine_mode,
    acceleration_mode,
    bi_engine_reasons,
    labels,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS jobs
  LEFT JOIN
    UNNEST(referenced_tables) AS referenced_table
),
jobs_by_project AS (
  {%- for project in DEFAULT_PROJECTS %}
    {%- if not loop.first %}
      UNION ALL
    {%- endif %}
    SELECT
      jp.project_id AS source_project,
      DATE(creation_time) AS creation_date,
      job_id,
      COALESCE(parent_job_id, job_id) AS script_job_id,
      REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
      REGEXP_EXTRACT(query, r'Query ID: (\w+), ') AS query_id,
      UPPER(
        LTRIM(REGEXP_REPLACE(query, r'\s+', ' '))
      ) LIKE 'CALL BQ.REFRESH_MATERIALIZED_VIEW%' AS is_materialized_view_refresh,
    FROM
      `{{project}}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS jp
    WHERE
      -- The previous day is included for parents of scripts that started before midnight
      DATE(creation_time)
      BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
      AND @submission_date
      AND (DATE(creation_time) = @submission_date OR statement_type = 'SCRIPT')
  {%- endfor %}
),
-- Child jobs of multi-statement queries don't have the Redash header, so it's taken from the parent
job_annotations AS (
  SELECT
    source_project,
    job_id,
    -- Only the parent's value is non-null inside the IF, so MAX returns it
    COALESCE(username, MAX(IF(job_id = script_job_id, username, NULL)) OVER script) AS username,
    COALESCE(query_id, MAX(IF(job_id = script_job_id, query_id, NULL)) OVER script) AS query_id,
    is_materialized_view_refresh,
  FROM
    jobs_by_project
  QUALIFY
    creation_date = @submission_date
  WINDOW
    script AS (
      PARTITION BY
        source_project,
        script_job_id
    )
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
  jo.labels,
  jp.is_materialized_view_refresh,
  jo.bi_engine_mode,
  jo.acceleration_mode,
  jo.bi_engine_reasons,
FROM
  jobs_by_org AS jo
LEFT JOIN
  job_annotations AS jp
  USING (source_project, job_id)
WHERE
  creation_date = @submission_date
