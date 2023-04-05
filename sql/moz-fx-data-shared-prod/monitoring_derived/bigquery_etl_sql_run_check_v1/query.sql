SELECT
  CURRENT_TIMESTAMP() AS last_run_timestamp,
  '@task_instance_key_str' AS task_instance,
  '@run_id' AS run_id
