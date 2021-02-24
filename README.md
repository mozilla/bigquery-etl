[![CircleCI](https://circleci.com/gh/mozilla/bigquery-etl.svg?style=shield&circle-token=742fb1108f7e6e5a28c11d43b21f62605037f5a4)](https://circleci.com/gh/mozilla/bigquery-etl)

BigQuery ETL
===

This repository contains Mozilla Data Team's
- Derived ETL jobs that do not require a custom container
- User-defined functions (UDFs)
- Airflow DAGs for scheduled bigquery-etl queries
- Tools for query & UDF deployment, management and scheduling

Quick Start
---

Ensure Python 3.8+ is available on your machine (see [this guide](https://docs.python-guide.org/starting/install3/osx/) for instructions if you're on a mac and haven't installed anything other than the default system Python.)

For some functionality Java JDK 8+ is also required, and maven is needed for downloading jar dependencies. If you don't already have a JDK and maven installed, consider using [jenv](https://www.jenv.be/) and the `jenv enable-plugin maven` command.

Install and set up the GCP command line tools:

* (For Mozilla Employees or Contributors not in Data Engineering) Set up GCP command line tools, [as described on docs.telemetry.mozilla.org](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#using-the-bq-command-line-tool). Note that some functionality (e.g. writing UDFs or backfilling queries) may not be allowed.
* (For Data Engineering) In addition to setting up the command line tools, you will want to log in to `shared-prod` if making changes to production systems. Run `gcloud auth login --project=moz-fx-data-shared-prod` and `gcloud auth application-default login` (if you have not run it previously).

Install the [virtualenv](https://virtualenv.pypa.io/en/latest/) Python environment management tool
```bash
pip install virtualenv
```

Clone the repository
```bash
git clone git@github.com:mozilla/bigquery-etl.git
cd bigquery-etl
```

Install the `bqetl` command line tool
```bash
./bqetl bootstrap
```

Install standard pre-commit hooks
```bash
venv/bin/pre-commit install
```

Optionally, download java dependencies
```bash
mvn dependency:copy-dependencies
```

And you should now be set up to start working in the repo! The easiest way to do this is for many tasks is to use `bqetl`, which is described below.


The `bqetl` CLI
---

The `bqetl` command-line tool aims to simplify working with the bigquery-etl repository by supporting
common workflows, such as creating, validating and scheduling queries or adding new UDFs.


### Usage

The CLI groups commands into different groups:

```
$ ./bqetl --help
Commands:
  dag     Commands for managing DAGs.
  dryrun  Dry run SQL.
  format  Format SQL.
  mozfun  Commands for managing mozfun UDFs.
  query   Commands for managing queries.
  udf     Commands for managing UDFs.
  ...
```

To get information about commands and available options, simply append the `--help` flag:

```
$ ./bqetl query create --help
Usage: bqetl query create [OPTIONS] NAME

  Create a new query with name <dataset>.<query_name>, for example:
  telemetry_derived.asn_aggregates

Options:
  -p, --path DIRECTORY  Path to directory in which query should be created
  -o, --owner TEXT      Owner of the query (email address)
  -i, --init            Create an init.sql file to initialize the table
  --help                Show this message and exit.
```

Running some commands, for example to create or query tables, will [require access to Mozilla's GCP Account](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#bigquery-access-request).

Formatting SQL
---

We enforce consistent SQL formatting as part of CI. After adding or changing a
query, use `script/format_sql` to apply formatting rules.

Directories and files passed as arguments to `script/format_sql` will be
formatted in place, with directories recursively searched for files with a
`.sql` extension, e.g.:

```bash
$ echo 'SELECT 1,2,3' > test.sql
$ script/format_sql test.sql
modified test.sql
1 file(s) modified
$ cat test.sql
SELECT
  1,
  2,
  3
```

If no arguments are specified the script will read from stdin and write to
stdout, e.g.:

```bash
$ echo 'SELECT 1,2,3' | script/format_sql
SELECT
  1,
  2,
  3
```

To turn off sql formatting for a block of SQL, wrap it in `format:off` and
`format:on` comments, like this:

```sql
SELECT
  -- format:off
  submission_date, sample_id, client_id
  -- format:on
```

Recommended practices
---

### Queries

- Should be defined in files named as `sql/<project>/<dataset>/<table>_<version>/query.sql` e.g.
  -  `<project>` defines both where the destination table resides and in which project the query job runs
  `sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v7/query.sql`
  - Queries that populate tables should always be named with a version suffix;
    we assume that future optimizations to the data representation may require
    schema-incompatible changes such as dropping columns
- May be generated using a python script that prints the query to stdout
  - Should save output as `sql/<project>/<dataset>/<table>_<version>/query.sql` as above
  - Should be named as `sql/<project>/query_type.sql.py` e.g. `sql/moz-fx-data-shared-prod/clients_daily.sql.py`
  - May use options to generate queries for different destination tables e.g.
    using `--source telemetry_core_parquet_v3` to generate
    `sql/moz-fx-data-shared-prod/telemetry/core_clients_daily_v1/query.sql` and using `--source main_summary_v4` to
    generate `sql/moz-fx-data-shared-prod/telemetry/clients_daily_v7/query.sql`
  - Should output a header indicating options used e.g.
    ```sql
    -- Query generated by: sql/moz-fx-data-shared-prod/clients_daily.sql.py --source telemetry_core_parquet
    ```
- Should not specify a project or dataset in table names to simplify testing
- Should be [incremental]
- Should filter input tables on partition and clustering columns
- Should use `_` prefix in generated column names not meant for output
- Should use `_bits` suffix for any integer column that represents a bit pattern
- Should not use `DATETIME` type, due to incompatibility with
  [spark-bigquery-connector]
- Should read from `*_stable` tables instead of including custom deduplication
  - Should use the earliest row for each `document_id` by `submission_timestamp`
    where filtering duplicates is necessary
- Should escape identifiers that match keywords, even if they aren't [reserved keywords]

### Views

- Should be defined in files named as `sql/<project>/<dataset>/<table>/view.sql` e.g.
  `sql/moz-fx-data-shared-prod/telemetry/core/view.sql`
  - Views should generally _not_ be named with a version suffix; a view represents a
    stable interface for users and whenever possible should maintain compatibility
    with existing queries; if the view logic cannot be adapted to changes in underlying
    tables, breaking changes must be communicated to `fx-data-dev@mozilla.org`
- Must specify project and dataset in all table names
  - Should default to using the `moz-fx-data-shared-prod` project;
    the `scripts/publish_views` tooling can handle parsing the definitions to publish
    to other projects such as `derived-datasets`

### UDFs

- Should limit the number of [expression subqueries] to avoid: `BigQuery error
  in query operation: Resources exceeded during query execution: Not enough
  resources for query planning - too many subqueries or query is too complex.`
- Should be used to avoid code duplication
- Must be named in files with lower snake case names ending in `.sql`
  e.g. `mode_last.sql`
  - Each file must only define effectively private helper functions and one
    public function which must be defined last
    - Helper functions must not conflict with function names in other files
  - SQL UDFs must be defined in the `udf/` directory and JS UDFs must be defined
    in the `udf_js` directory
    - The `udf_legacy/` directory is an exception which must only contain
      compatibility functions for queries migrated from Athena/Presto.
- Functions must be defined as [persistent UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#temporary-udf-syntax)
  using `CREATE OR REPLACE FUNCTION` syntax
  - Function names must be prefixed with a dataset of `<dir_name>.` so, for example,
    all functions in `udf/*.sql` are part of the `udf` dataset
    - The final syntax for creating a function in a file will look like
      `CREATE OR REPLACE FUNCTION <dir_name>.<file_name>`
  - We provide tooling in `scripts/publish_persistent_udfs` for
    publishing these UDFs to BigQuery
    - Changes made to UDFs need to be published manually in order for the
      dry run CI task to pass
- Should use `SQL` over `js` for performance

### Backfills

- Should be avoided on large tables
  - Backfills may double storage cost for a table for 90 days by moving
    data from long-term storage to short-term storage
    - For example regenerating `clients_last_seen_v1` from scratch would cost
      about $1600 for the query and about $6800 for data moved to short-term
      storage
  - Should combine multiple backfills happening around the same time
  - Should delay column deletes until the next other backfill
    - Should use `NULL` for new data and `EXCEPT` to exclude from views until
      dropped
- Should use copy operations in append mode to change column order
  - Copy operations do not allow changing partitioning, changing clustering, or
    column deletes
- Should split backfilling into queries that finish in minutes not hours
- May use [script/generate_incremental_table] to automate backfilling incremental
  queries
- May be performed in a single query for smaller tables that do not depend on history
  - A useful pattern is to have the only reference to `@submission_date` be a
    clause `WHERE (@submission_date IS NULL OR @submission_date = submission_date)`
    which allows recreating all dates by passing `--parameter=submission_date:DATE:NULL`

Incremental Queries
---

### Benefits

- BigQuery billing discounts for destination table partitions not modified in
  the last 90 days
- May use [dags.utils.gcp.bigquery_etl_query] to simplify airflow configuration
  e.g. see [dags.main_summary.exact_mau28_by_dimensions]
- May use [script/generate_incremental_table] to automate backfilling
- Should use `WRITE_TRUNCATE` mode or `bq query --replace` to replace
  partitions atomically to prevent duplicate data
- Will have tooling to generate an optimized _mostly materialized view_ that
  only calculates the most recent partition

### Properties

- Must accept a date via `@submission_date` query parameter
  - Must output a column named `submission_date` matching the query parameter
- Must produce similar results when run multiple times
  - Should produce identical results when run multiple times
- May depend on the previous partition
  - If using previous partition, must include an `init.sql` query to initialize the
    table, e.g. `sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_v1/init.sql`
  - Should be impacted by values from a finite number of preceding partitions
    - This allows for backfilling in chunks instead of serially for all time
      and limiting backfills to a certain number of days following updated data
    - For example `sql/moz-fx-data-shared-prod/clients_last_seen_v1.sql` can be run serially on any 28 day
      period and the last day will be the same whether or not the partition
      preceding the first day was missing because values are only impacted by
      27 preceding days

Query Metadata
---

- For each query, a `metadata.yaml` file should be created in the same directory
- This file contains a description, owners and labels. As an example:

```yaml
friendly_name: SSL Ratios
description: >
  Percentages of page loads Firefox users have performed that were
  conducted over SSL broken down by country.
owners:
  - example@mozilla.com
labels:
  application: firefox
  incremental: true     # incremental queries add data to existing tables
  schedule: daily       # scheduled in Airflow to run daily
  public_json: true
  public_bigquery: true
  review_bugs:
   - 1414839   # Bugzilla bug ID of data review
  incremental_export: false  # non-incremental JSON export writes all data to a single location
```

### Publishing Datasets

- To make query results publicly available, the `public_bigquery` flag must be set in
  `metadata.yaml`
  - Tables will get published in the `mozilla-public-data` GCP project which is accessible
    by everyone, also external users
- To make query results publicly available as JSON, `public_json` flag must be set in
  `metadata.yaml`
  - Data will be accessible under https://public-data.telemetry.mozilla.org
    - A list of all available datasets is published under https://public-data.telemetry.mozilla.org/all-datasets.json
  - For example: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000000000.json
  - Output JSON files have a maximum size of 1GB, data can be split up into multiple files (`000000000000.json`, `000000000001.json`, ...)
  - `incremental_export` controls how data should be exported as JSON:
    - `false`: all data of the source table gets exported to a single location
      - https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000000000.json
    - `true`: only data that matches the `submission_date` parameter is exported as JSON to a separate directory for this date
      - https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/2020-03-15/000000000000.json
- For each dataset, a `metadata.json` gets published listing all available files, for example: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/metadata.json
- The timestamp when the dataset was last updated is recorded in `last_updated`, e.g.: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/last_updated

Scheduling Queries in Airflow
---

- bigquery-etl has tooling to automatically generate Airflow DAGs for scheduling queries
- To be scheduled, a query must be assigned to a DAG that is specified in `dags.yaml`
  - New DAGs can be configured in `dags.yaml`, e.g., by adding the following:
  ```yaml
  bqetl_ssl_ratios:   # name of the DAG; must start with bqetl_
    schedule_interval: 0 2 * * *    # query schedule
    description: The DAG schedules SSL ratios queries.
    default_args:
      owner: example@mozilla.com
      start_date: '2020-04-05'  # YYYY-MM-DD
      email: ['example@mozilla.com']
      retries: 2    # number of retries if the query execution fails
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
    - `referenced_tables` - manually curated list of tables the query depends on; used to speed up the DAG generation process or to specify tables that the dry run doesn't have permissions to access, e. g. `[['telemetry_stable', 'main_v4']]`
    - `multipart` indicates whether a query is split over multiple files `part1.sql`, `part2.sql`, ...
    - `allow_field_addition_on_date`: date for which new fields are allowed to be added to the existing destination table query results are written to
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
      - `destination_table`: The table to write to. If unspecified, defaults to the query destination; if None, no destination table is used (the query is simply run as-is). Note that if no destination table is specified, you will need to specify the `submission_date` parameter manually
- Queries can also be scheduled using the `bqetl` CLI: `./bqetl query schedule path/to/query_v1 --dag bqetl_ssl_ratios `
- To generate all Airflow DAGs run `./script/generate_airflow_dags` or `./bqetl dag generate`
  - Generated DAGs are located in the `dags/` directory
  - Dependencies between queries scheduled in bigquery-etl and dependencies to stable tables are detected automatically
- Specific DAGs can be generated by running `./bqetl dag generate bqetl_ssl_ratios`
- Generated DAGs will be automatically detected and scheduled by Airflow
  - It might take up to 10 minutes for new DAGs and updates to show up in the Airflow UI

Contributing
---

When adding or modifying a query in this repository, make your changes in the `sql/` directory.

When adding a new library to the Python requirements, first add the library to
the requirements and then add any meta-dependencies into constraints.
Constraints are discovered by installing requirements into a fresh virtual
environment. A dependency should be added to either `requirements.txt` or
`constraints.txt`, but not both.

```bash
# Create a python virtual environment (not necessary if you have already
# run `./bqetl bootstrap`)
python3 -m venv venv/

# Activate the virtual environment
source venv/bin/activate

# If not installed:
pip install pip-tools

# Add the dependency to requirements.in e.g. Jinja2.
echo Jinja2==2.11.1 >> requirements.in

# Compile hashes for new dependencies.
pip-compile --generate-hashes requirements.in

# Deactivate the python virtual environment.
deactivate
```

Tests
---

[See the documentation in tests/](tests/README.md)

[script/generate_incremental_table]: https://github.com/mozilla/bigquery-etl/blob/master/script/generate_incremental_table
[expression subqueries]: https://cloud.google.com/bigquery/docs/reference/standard-sql/expression_subqueries
[dags.utils.gcp.bigquery_etl_query]: https://github.com/mozilla/telemetry-airflow/blob/89a6dc3/dags/utils/gcp.py#L364
[dags.main_summary.exact_mau28_by_dimensions]: https://github.com/mozilla/telemetry-airflow/blob/89a6dc3/dags/main_summary.py#L385-L390
[incremental]: #incremental-queries
[spark-bigquery-connector]: https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/5
[reserved keywords]: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved-keywords
[mozilla-pipeline-schemas]: https://github.com/mozilla-services/mozilla-pipeline-schemas
