SELECT
  dag_id,
  COALESCE(root_dag_id, dag_id) AS root_dag_id,
  IF(is_subdag, SPLIT(dag_id, ".")[OFFSET(1)], NULL) AS subdag_id,
  is_subdag,
  description,
  schedule_interval,
  owners,
  is_active,
  is_paused,
  has_task_concurrency_limits,
  max_active_runs,
  max_active_tasks,
  next_dagrun,
  next_dagrun_create_after,
  next_dagrun_data_interval_start,
  next_dagrun_data_interval_end,
  last_parsed_time,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag`
WHERE
  NOT _fivetran_deleted
  AND is_active
