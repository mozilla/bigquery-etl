# Scheduling Queries in Airflow

- bigquery-etl has tooling to automatically generate Airflow DAGs for scheduling queries
- To be scheduled, a query must be assigned to a DAG that is specified in `dags.yaml`
  - New DAGs can be configured in `dags.yaml`, e.g., by adding the following:
  ```yaml
  bqetl_ssl_ratios: # name of the DAG; must start with bqetl_
    schedule_interval: 0 2 * * * # query schedule
    description: The DAG schedules SSL ratios queries.
    default_args:
      owner: example@mozilla.com
      start_date: "2020-04-05" # YYYY-MM-DD
      email: ["example@mozilla.com"]
      retries: 2 # number of retries if the query execution fails
      retry_delay: 30m
  ```
  - All DAG names need to have `bqetl_` as prefix.
  - `schedule_interval` is either defined as a [CRON expression](https://en.wikipedia.org/wiki/Cron) or alternatively as one of the following [CRON presets](https://airflow.readthedocs.io/en/latest/dag-run.html): `once`, `hourly`, `daily`, `weekly`, `monthly`
  - `start_date` defines the first date for which the query should be executed
    - Airflow will not automatically backfill older dates if `start_date` is set in the past, backfilling can be done via the Airflow web interface
  - `email` lists email addresses alerts should be sent to in case of failures when running the query
- Alternatively, new DAGs can also be created via the `bqetl` CLI by running `bqetl dag create bqetl_ssl_ratios --schedule_interval='0 2 * * *' --owner="example@mozilla.com" --start_date="2020-04-05" --description="This DAG generates SSL ratios."`
- To schedule a specific query, add a `metadata.yaml` file that includes a `scheduling` section, for example:
  ```yaml
  friendly_name: SSL ratios
  # ... more metadata, see Query Metadata section above
  scheduling:
    dag_name: bqetl_ssl_ratios
  ```
  - Additional scheduling options:
    - `depends_on_past` keeps query from getting executed if the previous schedule for the query hasn't succeeded
    - `date_partition_parameter` - by default set to `submission_date`; can be set to `null` if query doesn't write to a partitioned table
    - `parameters` specifies a list of query parameters, e.g. `["n_clients:INT64:500"]`
    - `arguments` - a list of arguments passed when running the query, for example: `["--append_table"]`
    - `referenced_tables` - manually curated list of tables a Python or BigQuery script depends on; for `query.sql` files dependencies will get determined automatically and should only be overwritten manually if really necessary
    - `multipart` indicates whether a query is split over multiple files `part1.sql`, `part2.sql`, ...
    - `depends_on` defines external dependencies in telemetry-airflow that are not detected automatically:
    ```yaml
    depends_on:
      - task_id: external_task
        dag_name: external_dag
        execution_delta: 1h
    ```
      - `task_id`: name of task query depends on
      - `dag_name`: name of the DAG the external task is part of
      - `execution_delta`: time difference between the `schedule_intervals` of the external DAG and the DAG the query is part of
    - `depends_on_tables_existing` defines tables that the ETL will await the existence of via an Airflow sensor before running:
      ```yaml
      depends_on_tables_existing:
        - task_id: wait_for_foo_bar_baz
          table_id: 'foo.bar.baz_{{ ds_nodash }}'
          poke_interval: 30m
          timeout: 12h
          retries: 1
          retry_delay: 10m
      ```
      - `task_id`: ID to use for the generated Airflow sensor task.
      - `table_id`: Fully qualified ID of the table to wait for, including the project and dataset.
      - `poke_interval`: Time that the sensor should wait in between each check, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default poke interval is 5 minutes).
      - `timeout`: Time allowed before the sensor times out and fails, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default timeout is 8 hours).
      - `retries`: The number of retries that should be performed if the sensor times out or otherwise fails.
        This parameter is optional (the default depends on how the DAG is configured).
      - `retry_delay`: Time delay between retries, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default depends on how the DAG is configured).
    - `depends_on_table_partitions_existing` defines table partitions that the ETL will await the existence of via an Airflow sensor before running:
      ```yaml
      depends_on_table_partitions_existing:
        - task_id: wait_for_foo_bar_baz
          table_id: foo.bar.baz
          partition_id: '{{ ds_nodash }}'
          poke_interval: 30m
          timeout: 12h
          retries: 1
          retry_delay: 10m
      ```
      - `task_id`: ID to use for the generated Airflow sensor task.
      - `table_id`: Fully qualified ID of the table to check, including the project and dataset.
        Note that the service account `airflow-access@moz-fx-data-shared-prod.iam.gserviceaccount.com` will need to have the BigQuery Job User role on the project and read access to the dataset.
      - `partition_id`: ID of the partition to wait for.
      - `poke_interval`: Time that the sensor should wait in between each check, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default poke interval is 5 minutes).
      - `timeout`: Time allowed before the sensor times out and fails, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default timeout is 8 hours).
      - `retries`: The number of retries that should be performed if the sensor times out or otherwise fails.
        This parameter is optional (the default depends on how the DAG is configured).
      - `retry_delay`: Time delay between retries, formatted as a timedelta string like "2h" or "30m".
        This parameter is optional (the default depends on how the DAG is configured).
    - `trigger_rule`: The rule that determines when the airflow task that runs this query should run. The default is `all_success` ("trigger this task when all directly upstream tasks have succeeded"); other rules can allow a task to run even if not all preceding tasks have succeeded. See [the Airflow docs](https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html?highlight=trigger%20rule#trigger-rules) for the list of trigger rule options.
    - `destination_table`: The table to write to. If unspecified, defaults to the query destination; if None, no destination table is used (the query is simply run as-is). Note that if no destination table is specified, you will need to specify the `submission_date` parameter manually
    - `external_downstream_tasks` defines external downstream dependencies for which [`ExternalTaskMarker`s](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/external_task_sensor.html#externaltaskmarker) will be added to the generated DAG. These task markers ensure that when the task is cleared for triggering a rerun, all downstream tasks are automatically cleared as well.
    ```yaml
    external_downstream_tasks:
      - task_id: external_downstream_task
        dag_name: external_dag
        execution_delta: 1h
    ```
- Queries can also be scheduled using the `bqetl` CLI: `./bqetl query schedule path/to/query_v1 --dag bqetl_ssl_ratios `
- To generate all Airflow DAGs run `./bqetl dag generate`
  - Generated DAGs are located in the `dags/` directory
  - Dependencies between queries scheduled in bigquery-etl and dependencies to stable tables are detected automatically
- Specific DAGs can be generated by running `./bqetl dag generate bqetl_ssl_ratios`
- Generated DAGs do not need to be checked into `main`. CI automatically generates DAGs and writes them to the [telemetry-airflow-dags](https://github.com/mozilla/telemetry-airflow-dags) repo from where Airflow will pick them up
- Generated DAGs will be automatically detected and scheduled by Airflow
  - It might take up to 10 minutes for new DAGs and updates to show up in the Airflow UI
- To generate tasks for importing data from Fivetran that an ETL task depends on add:
  ```yaml
  depends_on_fivetran:
    - task_id: fivetran_import_1
    - task_id: another_fivetran_import
  ```
  - The Fivetran connector ID needs to be set as a variable `<task_id>_connector_id` in the Airflow admin interface for each import task
