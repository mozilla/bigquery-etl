{% set DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
] %}

WITH jobs_by_org AS (
  SELECT
    t1.project_id AS source_project,
    creation_date,
    job_id,
    job_type,
    reservation_id,
    cache_hit,
    state,
    statement_type,
    referenced_tables.project_id AS reference_project_id,
    referenced_tables.dataset_id AS reference_dataset_id,
    referenced_tables.table_id AS reference_table_id,
    destination_table.project_id AS  destination_project_id,
    destination_table.dataset_id AS  destination_dataset_id,
    destination_table.table_id AS  destination_table_id,
    user_email,
    end_time-start_time as task_duration,
    ROUND(total_bytes_processed / 1024 / 1024 / 1024 / 1024, 4)
    AS total_terabytes_processed,
    ROUND(total_bytes_billed / 1024 / 1024 / 1024 / 1024, 4)
    AS total_terabytes_billed,
    total_slot_ms,
    error_result.location AS error_location,
    error_result.reason AS error_reason,
    error_result.message AS error_message,
    query_info_resource_warning AS resource_warning,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS t1
  LEFT JOIN
    UNNEST(referenced_tables) AS referenced_table
  ),
  {#- format off #}
  jobs_by_project AS (
  {%- for project in DEFAULT_PROJECTS %}
      SELECT
        jp.project_id AS source_project,
        date(creation_time) as creation_date,
        job_id,
        referenced_tables.project_id AS reference_project_id,
        referenced_tables.dataset_id AS reference_dataset_id,
        referenced_tables.table_id AS reference_table_id,
        user_email,
        REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
        REGEXP_EXTRACT(query, r'Query ID: (\\w+), ') AS query_id,
      FROM
        `{{project}}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` jp
      LEFT JOIN
        UNNEST(referenced_tables) AS referenced_tables
      WHERE
        DATE(creation_time) = @submission_date
  {%- if not loop.last %}
    UNION ALL
  {%- endif %}
  {%- endfor %}
      {#- format on #}
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
      FROM jobs_by_org jo
      LEFT JOIN jobs_by_project jp
      USING(source_project,
          creation_date,
          job_id,
          reference_project_id,
          reference_dataset_id,
          reference_table_id)
      WHERE creation_date = @submission_date
