# How to Run Tests

This repository uses `pytest`:

```bash
# create a venv
python3.11 -m venv venv/

# install pip-tools for managing dependencies
./venv/bin/pip install pip-tools -c requirements.in

# install python dependencies with pip-sync (provided by pip-tools)
./venv/bin/pip-sync --pip-args=--no-deps requirements.txt

# run pytest with all linters and 8 workers in parallel
./venv/bin/pytest --black --flake8 --isort --mypy-ignore-missing-imports --pydocstyle -n 8

# use -k to selectively run a set of tests that matches the expression `udf`
./venv/bin/pytest -k udf

# pass specific paths for quicker turnaround when running particular SQL tests
./venv/bin/pytest sql/moz-fx-data-shared-prod/search_derived/mobile_search_aggregates_v1/tests

# run integration tests with 4 workers in parallel
gcloud auth application-default login # or set GOOGLE_APPLICATION_CREDENTIALS
export GOOGLE_PROJECT_ID=moz-fx-data-integration-tests
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
in `sql/mozfun/assert/` may be used to evaluate outputs. Tests must not use any
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
  mozfun.assert.true(udf_example(1)),
  mozfun.assert.false(udf_example(0));
#xfail
SELECT
  udf_example(-1);
#xfail
SELECT
  udf_example(NULL);
```

[sql dialect prefix]: https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql#sql-prefix

## How to Configure an ETL/View SQL Test

ETLs/views are tested by running the associated SQL query against test versions of source tables/views and comparing the result with the expected data.
Only ETLs with a `query.sql` file and views with a `view.sql` file are supported (not `script.sql` or `query.py` ETLs).
Tests for ETLs/views created by SQL generators are skipped unless that SQL has been generated (e.g. via `./bqetl generate all`).

1. Add a `tests` subdirectory inside the ETL/view directory if one doesn't already exist.
2. Add a YAML file for the test inside the `tests` subdirectory.
   - The test file name should start with `test_`, use underscores to separate words, and end with a `.yaml` extension.
     - e.g. `sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_v1/tests/test_single_day.yaml`
3. Add a dictionary for source tables/views under `sources:` in the YAML file.
   - Each key should be the fully qualified ID of the source table/view.
   - Each value should be a list of dictionaries representing the rows in the source table/view.
     - For tables/views with resolvable schemas, fields with null values can be omitted from the dictionaries for brevity.
4. Add a list of query parameters under `parameters:` in the YAML file.
   - Each parameter should be specified as a dictionary with `name`, `type`, and `value` keys.
5. If the ETL query contains `is_init()` logic, then add an `is_init` key in the YAML file with a boolean value specifying what `is_init()` should return during the test.
6. Add the expected result data as a list of dictionaries under `expect:` in the YAML file.
   - `DATE`, `DATETIME`, and `TIMESTAMP` fields in the result are coerced to strings using `.isoformat()`.
   - Columns named `created_at` or `generated_time` are removed from the result before comparing to `expect` because they aren't expected to be static.
   - Fields with null values should be omitted from the expected result data (so be careful with spreading previous rows like `<<: *base` in that regard).
7. Optionally add schema files for source table/view schemas in the `tests` directory.
   - The schema file names should start with the fully qualified ID of the source table/view, and end with a `.schema.yaml` extension.
     - e.g. `sql/moz-fx-data-shared-prod/customer_experience_derived/zendesk_retrieval_index_v1/tests/moz-fx-data-shared-prod.zendesk_syndicate.ticket.schema.yaml`
   - Such table/view schemas apply to all tests for the ETL/view.
   - Setting the description of a top-level field to `time_partitioning_field` will cause the table to use it for time partitioning.
   - If a source table/view has no schema file in the `tests` directory then `bqetl` will automatically try to retrieve the schema from the generated live/stable table schemas or ETL/view `schema.yaml` files that exist (if any), and if no schema could be retrieved BigQuery falls back to inferring the schema from the source data.
     - For live/stable tables and ETLs/views with `schema.yaml` files it's strongly recommended to rely on the automatic schema retrieval, since that helps keep the test accurate as schemas change over time.

Example `test_*.yaml` file:
```yaml
sources:
  moz-fx-data-shared-prod.firefox_desktop_derived.desktop_installs_v1:
    - submission_date: 2020-01-02
      client_id: abc
    - submission_date: 2020-01-02
      client_id: def

parameters:
  - name: submission_date
    type: DATE
    value: 2020-01-02

is_init: false

expect:
  - submission_date: 2020-01-02
    client_count: 2
```

## Additional Guidelines and Options

- If the destination table is also an input table then `generated_time` should
  be a required `DATETIME` field to ensure minimal validation
