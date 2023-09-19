# How to Run Tests

This repository uses `pytest`:

```bash
# create a venv
python3.10 -m venv venv/

# install pip-tools for managing dependencies
./venv/bin/pip install pip-tools -c requirements.in

# install python dependencies with pip-sync (provided by pip-tools)
./venv/bin/pip-sync --pip-args=--no-deps requirements.txt

# run pytest with all linters and 8 workers in parallel
./venv/bin/pytest --black --flake8 --isort --mypy-ignore-missing-imports --pydocstyle -n 8

# use -k to selectively run a set of tests that matches the expression `udf`
./venv/bin/pytest -k udf

# narrow down testpaths for quicker turnaround when selecting a single test
./venv/bin/pytest -o "testpaths=tests/sql" -k mobile_search_aggregates_v1

# run integration tests with 4 workers in parallel
gcloud auth application-default login # or set GOOGLE_APPLICATION_CREDENTIALS
export GOOGLE_PROJECT_ID=bigquery-etl-integration-test
gcloud config set project $GOOGLE_PROJECT_ID
./venv/bin/pytest -m integration -n 4
```

To provide [authentication credentials for the Google Cloud API](https://cloud.google.com/docs/authentication/getting-started) the `GOOGLE_APPLICATION_CREDENTIALS` environment variable must be set to the file path of the JSON file that contains the service account key.
See [Mozilla BigQuery API Access instructions](https://docs.telemetry.mozilla.org/cookbooks/bigquery.html#gcp-bigquery-api-access) to request credentials if you don't already have them.

## How to Configure a UDF Test

Include a comment like `-- Tests` followed by one or more query statements
after the UDF in the SQL file where it is defined. Each statement in a SQL file
that defines a UDF that does not define a temporary function is collected as a
test and executed independently of other tests in the file.

Each test must use the UDF and throw an error to fail. Assert functions defined
in `tests/assert/` may be used to evaluate outputs. Tests must not use any
query parameters and should not reference any tables. Each test that is
expected to fail must be preceded by a comment like `#xfail`, similar to a [SQL
dialect prefix] in the BigQuery Cloud Console.

For example:

```sql
CREATE TEMP FUNCTION udf_example(option INT64) AS (
  CASE
  WHEN option > 0 then TRUE
  WHEN option = 0 then FALSE
  ELSE ERROR("invalid option")
  END
);
-- Tests
SELECT
  assert_true(udf_example(1)),
  assert_false(udf_example(0));
#xfail
SELECT
  udf_example(-1);
#xfail
SELECT
  udf_example(NULL);
```

[sql dialect prefix]: https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql#sql-prefix

## How to Configure a Generated Test

Queries are tested by running the `query.sql` with test-input tables and comparing the result to an expected table.
1. Make a directory for test resources named `tests/sql/{project}/{dataset}/{table}/{test_name}/`,
   e.g. `tests/sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_raw_v1/test_single_day`
   - `table` must match a directory named like `{dataset}/{table}`, e.g.
     `telemetry_derived/clients_last_seen_v1`
   - `test_name` should start with `test_`, e.g. `test_single_day`
   - If `test_name` is `test_init` or `test_script`, then the query will run `init.sql`
     or `script.sql` respectively; otherwise, the test will run `query.sql`
1. Add `.yaml` files for input tables, e.g. `clients_daily_v6.yaml`
   - Include the dataset prefix if it's set in the tested query,
     e.g. `analysis.clients_last_seen_v1.yaml`
   - Include the project prefix if it's set in the tested query,
     e.g. `moz-fx-other-data.new_dataset.table_1.yaml`
     - This will result in the dataset prefix being removed from the query,
       e.g. `query = query.replace("analysis.clients_last_seen_v1", "clients_last_seen_v1")`
1. Add `.sql` files for input view queries, e.g. `main_summary_v4.sql`
   - **_Don't_** include a `CREATE ... AS` clause
   - Fully qualify table names as `` `{project}.{dataset}.table` ``
   - Include the dataset prefix if it's set in the tested query,
     e.g. `telemetry.main_summary_v4.sql`
     - This will result in the dataset prefix being removed from the query,
       e.g. `query = query.replace("telemetry.main_summary_v4", "main_summary_v4")`
1. Add `expect.yaml` to validate the result
   - `DATE` and `DATETIME` type columns in the result are coerced to strings
     using `.isoformat()`
   - Columns named `generated_time` are removed from the result before
     comparing to `expect` because they should not be static
   - `NULL` values should be omitted in `expect.yaml`. If a column is expected to be `NULL` don't add it to `expect.yaml`.
     (Be careful with spreading previous rows (`-<<: *base`) here)
1. Optionally add `.schema.json` files for input table schemas to the table directory, e.g.
   `tests/sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_raw_v1/clients_daily_v6.schema.json`.
   These tables will be available for every test in the suite.
   The `schema.json` file need to match the table name in the `query.sql` file. If it has project and dataset listed there, the schema file also needs project and dataset.
1. Optionally add `query_params.yaml` to define query parameters
   - `query_params` must be a list

## Init Tests

Tests of `init.sql` statements are supported, similarly to other generated tests.
Simply name the test `test_init`. The other guidelines still apply.

_Note_: Init SQL statements must contain a create statement with the dataset
and table name, like so:

```sql
CREATE OR REPLACE TABLE
  dataset.table_v1
AS
...
```

## Additional Guidelines and Options

- If the destination table is also an input table then `generated_time` should
  be a required `DATETIME` field to ensure minimal validation
- Input table files
  - All of the formats supported by `bq load` are supported
  - `yaml` and `json` format are supported and must contain an array of rows
    which are converted in memory to `ndjson` before loading
  - Preferred formats are `yaml` for readability or `ndjson` for compatiblity
    with `bq load`
- `expect.yaml`
  - File extensions `yaml`, `json` and `ndjson` are supported
  - Preferred formats are `yaml` for readability or `ndjson` for compatiblity
    with `bq load`
- Schema files
  - Setting the description of a top level field to `time_partitioning_field`
    will cause the table to use it for time partitioning
  - File extensions `yaml`, `json` and `ndjson` are supported
  - Preferred formats are `yaml` for readability or `json` for compatiblity
    with `bq load`
- Query parameters
  - Scalar query params should be defined as a dict with keys `name`, `type` or
    `type_`, and `value`
  - `query_parameters.yaml` may be used instead of `query_params.yaml`, but
    they are mutually exclusive
  - File extensions `yaml`, `json` and `ndjson` are supported
  - Preferred format is `yaml` for readability

## How to Run CircleCI Locally

- Install the [CircleCI Local CI](https://circleci.com/docs/2.0/local-cli/)
- Download GCP [service account](https://cloud.google.com/iam/docs/service-accounts) keys
  - Integration tests will only successfully run with service account keys
    that belong to the `circleci` service account in the `biguqery-etl-integration-test` project
- Run `circleci build` and set required environment variables `GOOGLE_PROJECT_ID` and
  `GCLOUD_SERVICE_KEY`:

```bash
gcloud_service_key=`cat /path/to/key_file.json`

# to run a specific job, e.g. integration:
circleci build --job integration \
  --env GOOGLE_PROJECT_ID=bigquery-etl-integration-test \
  --env GCLOUD_SERVICE_KEY=$gcloud_service_key

# to run all jobs
circleci build \
  --env GOOGLE_PROJECT_ID=bigquery-etl-integration-test \
  --env GCLOUD_SERVICE_KEY=$gcloud_service_key
```
