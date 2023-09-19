SELECT
  CURRENT_TIMESTAMP() AS run_timestamp,
  @task_instance AS task_instance,
  @run_id AS run_id
