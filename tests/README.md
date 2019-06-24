How to Run Tests
===

This repository uses `pytest`:

```
# create a venv
python3.7 -m venv venv/

# install requirements
venv/bin/pip install -r requirements.txt

# run pytest with all linters and 4 workers in parallel
venv/bin/pytest --black --docstyle --flake8 --mypy-ignore-missing-imports -n 4
```

To provide [authentication credentials for the Google Cloud API](https://cloud.google.com/docs/authentication/getting-started) the `GOOGLE_APPLICATION_CREDENTIALS` environment variable must be set to the file path of the JSON file that contains the service account key.
See [Mozilla BigQuery API Access instructions](https://docs.telemetry.mozilla.org/cookbooks/bigquery.html#gcp-bigquery-api-access) to request credentials if you don't already have them.

How to Configure a Generated Test
===

1. Make a directory for test resources named `tests/{query_name}/{test_name}/`,
   e.g. `tests/clients_last_seen_v1/test_single_day`
   - `query_name` must match a query file named `sql/{query_name}.sql`, e.g.
     `sql/clients_last_seen_v1.sql`
   - `test_name` should start with `test_`, e.g. `test_single_day`
1. Add `.ndjson` files for input tables, e.g. `clients_daily_v6.ndjson`
   - Include the dataset prefix if it's set in the tested query,
     e.g. `analysis.clients_last_seen_v1.ndjson`
     - This will result in the dataset prefix being removed from the query,
       e.g. `query.replace("analysis.clients_last_seen_v1",
       "clients_last_seen_v1")`
1. Add `expect.ndjson` to validate the result
   - `DATE` and `DATETIME` type columns in the result are coerced to strings
     using `.isoformat()`
   - Columns named `generated_time` are removed from the result before
     comparing to `expect` because they should not be static
1. Optionally add `.schema.json` files for input table schemas, e.g.
   `clients_daily_v6.schema.json`
1. Optionally add `query_params.yaml` to define query parameters
   - `query_params` must be a list

Additional Guidelines and Options
---

- If the destination table is also an input table then `generated_time` should
  be a required `DATETIME` field to ensure minimal validation
- Input table files
   - All of the formats supported by `bq load` are supported
   - Formats other than `.ndjson` and `.csv` should not be used because they
     are not human readable
- `expect.ndjson`
   - File extensions `yaml`, `json` and `ndjson` are supported
   - Formats other than `ndjson` should not be used because they are not
     supported by `bq load`
- Schema files
  - Setting the description of a top level field to `time_partitioning_field`
    will cause the table to use it for time partitioning
  - File extensions `yaml`, `json` and `ndjson` are supported
  - Formats other than `.json` should not be used because they are not
    supported by `bq load`
- Query parameters
  - Scalar query params should be defined as a dict with keys `name`, `type` or
    `type_`, and `value`
  - `query_parameters.yaml` may be used instead of `query_params.yaml`, but
    they are mutually exclusive
  - File extensions `yaml`, `json` and `ndjson` are supported
