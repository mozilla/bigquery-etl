# Backfilling a table

**Note** For large sets of data, follow the [recommended practices](https://mozilla.github.io/bigquery-etl/reference/recommended_practices/#backfills) for backfills.

## What can be backfilled with `bqetl backfill`

The managed backfill workflow (`bqetl backfill create` / `initiate`) validates a table's `metadata.yaml` and the backfill entry before it will run. Some table configurations are rejected outright (no override), some are allowed only with an explicit override flag, and some are supported with special handling. The table below summarizes the rules enforced by the backfill code.

| Table / backfill configuration                                                                     | Allowed?                            | Example                                                                                                                                                                                                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------------------------------------------------------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Standard incremental table, day-partitioned, with a `date_partition_parameter` set                 | ✅ Yes                               | [`addons_derived.amo_stats_dau_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/addons_derived/amo_stats_dau_v1)                                                                             | The common case. `time_partitioning.type: day` with a non-null `date_partition_parameter` (defaults to `submission_date`). Each partition is processed independently.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Month partitioning (`time_partitioning.type: month`)                                               | ✅ Yes                               | [`subscription_platform_derived.monthly_active_logical_subscriptions_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/subscription_platform_derived/monthly_active_logical_subscriptions_v1) | Monthly partitioning is supported (`bigquery.time_partitioning.type: month`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Unpartitioned table (no `time_partitioning`)                                                       | ✅ Yes                               | [`messaging_system.cfr_users_daily`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/messaging_system/cfr_users_daily)                                                                           | The partitioning check only restricts the type when partitioning is set.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Generated SQL (table produced by a `sql_generators/` template)                                     | ✅ Yes                               | [`org_mozilla_firefox_derived.baseline_clients_daily_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/baseline_clients_daily_v1)                                 | Backfills act on the materialized files under `sql/<project>/<dataset>/<table>/`, which are generated into the deployed `sql/` tree (from the `generated-sql` branch) when running the backfill, even though they aren't committed on `main`. In this example only the [`backfill.yaml`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/baseline_clients_daily_v1/backfill.yaml) is committed; the SQL is generated by [`sql_generators/glean_usage/baseline_clients_daily.py`](https://github.com/mozilla/bigquery-etl/tree/main/sql_generators/glean_usage/baseline_clients_daily.py). |
| `depends_on_past: true` **with** a non-null `date_partition_parameter`                             | ✅ Yes (special handling)            | [`firefox_accounts_derived.fxa_users_services_devices_first_seen_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_users_services_devices_first_seen_v1)         | The query is rewritten to read from the staging table, and the prior partition is seeded into staging first. The entry's `end_date` must be on or after the `entry_date` (override by setting `override_depends_on_past_end_date: true` on the entry).                                                                                                                                                                                                                                                                                                                                                                                              |
| `query.py` (Python script) table                                                                   | ✅ Yes (with extra config)           | [`monitoring_derived.stable_and_derived_table_sizes_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/monitoring_derived/stable_and_derived_table_sizes_v1)                                   | Requires `query_script_entrypoint` and `query_script_date_arg` on the entry. The staging table and dry run are not auto-configured; see [Backfilling with a Python script](#backfilling-with-a-python-script).                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `shredder_mitigation: true` label on the table                                                     | ✅ Yes (field required)              | [`telemetry_derived.desktop_retention_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/telemetry_derived/desktop_retention_v1)                                                               | The entry must have `shredder_mitigation: true`; otherwise `initiate` stops. The metadata label and the entry's `shredder_mitigation` value must match.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Workgroup-restricted (access-controlled) dataset                                                   | ✅ Yes                               |                                                                                                                                                                                                                                | The staging and backup tables will mirror the prod table's IAM policy and its dataset access.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **`depends_on_past: true` with a null `date_partition_parameter`**                                 | ✅ Yes (with `--reinitialize-table`) | [`telemetry_derived.clients_first_seen_v3`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/telemetry_derived/clients_first_seen_v3)                                                             | A whole-table refresh that depends on its own prior state can't be backfilled one partition at a time, so the normal day-by-day path is rejected by `validate_depends_on_past`. Instead, pass `--reinitialize-table` to rebuild the whole table from its `is_init()` query (the same logic `bqetl query initialize` uses), run in parallel per `sample_id` into staging and swapped in as a full-table replace on complete. Requires the table's `query.sql` to use the `is_init()` pattern. See [Reinitializing a whole table](#reinitializing-a-whole-table).                                                                                     |
| Start date older than the retention limit (smaller of 775 days or the partition `expiration_days`) | ⚠️ Override required                |                                                                                                                                                                                                                                | Rejected unless `override_retention_limit: true` is set on the entry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `depends_on_past: true` with `end_date` before the `entry_date`                                    | ⚠️ Override required                |                                                                                                                                                                                                                                | Rejected unless `override_depends_on_past_end_date: true` is set on the entry; a past end date can cause data inconsistencies.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Entry date older than 28 days (`MAX_BACKFILL_ENTRY_AGE_DAYS`)                                      | ❌ No                                |                                                                                                                                                                                                                                | An `Initiate`-status entry this old will not run (staging tables expire). Create a fresh entry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| More than one `Initiate`-status entry in a single `backfill.yaml`                                  | ❌ No                                |                                                                                                                                                                                                                                | Only one in-flight backfill per table is allowed at a time.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Partitioning type other than day or month (e.g. hour, year, or integer range)                      | ❌ No (no override)                  |                                                                                                                                                                                                                                | Rejected by `validate_partitioning_type`; only day and month partitioning are supported.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

The rejections are enforced by [`validate_table_metadata`](https://github.com/mozilla/bigquery-etl/tree/main/bigquery_etl/backfill/utils.py) in `bigquery_etl/backfill/utils.py`; the date/retention/duplicate checks live in [`bigquery_etl/backfill/validate.py`](https://github.com/mozilla/bigquery-etl/tree/main/bigquery_etl/backfill/validate.py).

## Testing a backfill in a dev environment

We can't create tables in `moz-fx-data-shared-prod.backfills_staging_derived`, so running
the full managed backfill workflow locally is only possible with `--target`, which stages into 
a test project. We can write to tables that already exist in
`moz-fx-data-shared-prod.backfills_staging_derived`, so an active backfill can be
amended by writing to its existing staging table.

Before running a managed backfill against production, you can exercise the full
`initiate` and `complete` flow against a [development target](development_workflows.md) by
passing the global `--target` flag. With a target active, the backfill operates on the
target-deployed table and stages into the target project (e.g. your sandbox).
The query does not change, so it will still read from production tables:

```bash
# Deploy the table into your dev target first (see Development Workflows), then:
./bqetl --target dev backfill initiate moz-fx-data-shared-prod.monitoring_derived.shredder_per_job_stats_v1
# validate the staged data, then:
./bqetl --target dev backfill complete moz-fx-data-shared-prod.monitoring_derived.shredder_per_job_stats_v1
```

What changes under `--target`:

- Staging and backup tables are created in the **target project**'s
  `backfills_staging_derived` dataset instead of production's.
- The table that gets backed up and swapped is the **target-deployed** copy of the table,
  not the production table.
- Queries bill to the target project, and production IAM mirroring is skipped.

The `backfill.yaml` entry is still read from the repo as usual; only the BigQuery
locations are redirected. Without `--target`, behavior is unchanged (production backfill).

`query.py` (python script) backfills do not support `--target`. A python-script backfill's
destination is baked into the entry's `query_script_args` at create time and is not redirected, 
so running one under `--target` is rejected to avoid writing to the production staging table. 
This limitation is tracked in https://mozilla-hub.atlassian.net/browse/DENG-10054.

## Initiating the backfill:

1. Create a backfill schedule entry to (re)-process data in your table:

  ```bash
  bqetl backfill create
  ```
  Then fill out the prompts. A backfill can also be created from a single command:
  ```bash
  bqetl backfill create <project>.<dataset>.<table> --start_date=<YYYY-MM-DD> --end_date=<YYYY-MM-DD>
  ```

  - If the table's metadata has the label `shredder_mitigation: true`, use the process to run a [backfill with shredder_mitigation](https://docs.telemetry.mozilla.org/cookbooks/data_modeling/shredder_mitigation#running-a-managed-backfill-with-shredder-mitigation):
    For new tables:
      - Set `shredder_mitigation: false` since there is no data yet to safeguard.
      - Backfill and validate your data.
      - Set `shredder_mitigation: true` to protect the validated data. 
    For existing tables:
      - Bump the version of the query.
      - Make the necessary updates to the new version of the query and schema.
      - Create the managed backfill for the new version of the query with `shredder_mitigation: true` on the entry.
          ```bash
          bqetl backfill create <project>.<dataset>.<table> --start_date=<YYYY-MM-DD> --end_date=<YYYY-MM-DD> --shredder_mitigation
          ```

2. Fill out the missing details:
  - Watchers: Mozilla Emails for users that should be notified via Slack about backfill progress.
    - Note that the email name should match the username listed here: https://mozilla.slack.com/account/settings#username.
      If it doesn't, put the username with `@mozilla.com` instead (an email won't be sent there).
      e.g. if your username is `abcdef`, set the watcher to `abcdef@mozilla.com`
  - Reason: Why are you backfilling this table?

3. Open a Pull Request with the backfill entry, see [this example](https://github.com/mozilla/bigquery-etl/pull/5369). Once merged, you should receive a notification in around an hour that processing has started. Your backfill data will be temporarily placed in a staging location.

4. Watchers need to join the #dataops-alerts Slack channel. They will be notified via Slack when processing is complete, and you can validate your backfill data.

**Something to watch out for:** If the `end_date` of a backfill is the current day (e.g. `end_date = entry_date`)
and the backfill is started, the ETL for the upstream tables likely will not have run yet, causing the backfill to
create an empty partition. If the backfill is completed on a later day, it will copy the empty partition into the
production table, causing an empty partition in production.

## Backfilling with a Python script:

Tables that use a `query.py` file instead of `query.sql` are also supported with backfills, but have additional considerations.

**Importantly, backfills for scripts will not automatically configure the backfill staging table and dry run.**
The query.py must support destination table and dry run arguments, and the backfill must be configured to use them
if you would like to use them. If a destination table is not provided, the backfill will use the script's
default values, likely writing to the production table.

In order to use the backfill complete step, the script must write to the correct table in the backfill staging
dataset: `{dataset}__{table_name}_{backfill_date}`. e.g. setting
`--query-script-arg "--destination_table=monitoring_derived__stable_and_derived_table_sizes_v1_2026_03_02"`
Otherwise, the backfill complete will do nothing.

Required parameters:
- `query_script_entrypoint`: The name of the main function inside the python script.
- `query_script_date_arg`: The name of the CLI argument that the entrypoint accepts for the backfill date, formatted as `YYYY-MM-DD`
(e.g. `submission_date`). The backfill will pass each backfilled date to the script via this argument.

Optional parameters for Python scripts:
- `query_script_args`: Additional CLI arguments to pass to the script, e.g. `--project=moz-fx-data-shared-prod`.
Use this to set the backfill staging table if needed, e.g. `--destination_table=dataset__table_v1_YYYY_MM_DD`.
- `query_script_dry_run_arg`: The name of the CLI argument the script uses for a dry run, e.g. `--dry-run`.
When provided, the system runs the script once with this argument appended before running the real backfill, mirroring the SQL dry run behaviour. 
The script must implement support for this argument itself.

Example:
```bash
bqetl backfill create moz-fx-data-shared-prod.monitoring_derived.stable_and_derived_table_sizes_v1 \
    --start-date 2026-02-24 \
    --end-date 2026-02-26 \
    --exclude 2026-02-25 \
    --watcher nobody@mozilla.com \
    --query-script-entrypoint main \
    --query-script-date-arg date \
    --query-script-dry-run-arg "--dry-run" \
    --query-script-arg "--destination_dataset=backfills_staging_derived" \
    --query-script-arg "--destination_table=monitoring_derived__stable_and_derived_table_sizes_v1_2026_03_02"
```

## Reinitializing a whole table:

Tables with `depends_on_past: true` and a null `date_partition_parameter` (e.g. the
`first_seen` tables like `telemetry_derived.clients_first_seen_v3`) rewrite
the entire table on every scheduled run, with each day reading its own prior output. They
cannot be backfilled one partition at a time, so the normal day-by-day backfill path is
rejected.

Instead, create the backfill with `--reinitialize-table`:

```bash
bqetl backfill create moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3 \
    --start-date 2016-03-12 \
    --end-date 2026-06-03 \
    --watcher nobody@mozilla.com \
    --reason "Rebuild (DENG-12345)" \
    --reinitialize-table
```

This rebuilds the whole table from its `is_init()` query (the same logic used by
`bqetl query initialize`) rather than replaying the day-by-day query. On `initiate`, the
init query runs in parallel per `sample_id` into the backfill staging table; on
`complete`, the staging table replaces the production table in full,
rather than partition-by-partition over the start/end range.

Requirements and notes:

- The table's `query.sql` must use the `is_init()` pattern (the backfill is rejected
  otherwise).
- The `start_date`/`end_date` still bound the entry, but a reinitialize
  rebuilds every partition the `is_init()` query produces, not just that range.
- For tables sharded by `sample_id`, parallelism can be batched with the `@sampling_batch_size`
  parameter, rather than one job per `sample_id`. BigQuery limits partition modifications to 30000 
  per table per day. A first_seen table like clients_first_seen can have thousands of partitions, 
  and each per-sample_id query writes to most/all of them. Batching is required to reduce the 
  number of modifications so partition_modifications = num_batches * num_partitions.
  - The batch size defaults to 20 (5 jobs over 100 sample IDs), which keeps a ~3,700-partition 
    table under the 30,000/day cap with headroom for its daily scheduled DAG run; override it with
  `--reinitialize-sampling-batch-size`
  - To use this, the query must constrain `sample_id` with a range clause so a batch of IDs is
    processed per job:

  ```sql
  AND sample_id >= @sample_id
  AND sample_id < @sample_id + @sampling_batch_size
  ```

## Completing the backfill:

1. Validate that the backfill data looks like what you expect (calculate important metrics, look for nulls, etc.)
   - Note that backfill tables have a default of expiry of 30 days, so validation should be completed within 30 days of the start of the backfill

2. If the data is valid, open a Pull Request, setting the backfill status to Complete, see [this example](https://github.com/mozilla/bigquery-etl/pull/5352). Once merged, you should receive a notification in around an hour that swapping has started. Current production data will be backed up and the staging backfill data will be swapped into production.

3. You will be notified when swapping is complete.


**Note**. If your backfill is complex (backfill validation fails for e.g.), it is recommended to talk to someone in Data Engineering or Data SRE (#data-help) to process the backfill via the backfill DAG.
