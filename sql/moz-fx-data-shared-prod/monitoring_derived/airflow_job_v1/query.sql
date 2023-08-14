SELECT
  id,
  dag_id,
  end_date,
  executor_class,
  hostname,
  job_type,
  latest_heartbeat,
  start_date,
  state,
  unixname
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.job`
WHERE
  NOT _fivetran_deleted
