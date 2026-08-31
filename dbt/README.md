# a dbt development configuration inside bigquery-etl

This directory is for building data models in a development environment using [dbt](https://docs.getdbt.com/).
It exists to unlock dbt features for development that don't yet exist in bigquery-etl

> ⚠️ dbt is **not** wired into Airflow or CI, and it does not deploy
> to production. Models are written to your personal BigQuery sandbox project.

Dataset/table **mirrors bigquery-etl naming** (`<table>_vN`, organized by dataset

## Project structure

```
dbt/
├── dbt_project.yml                 # Project config (name: dbt_dev)
├── profiles.yml.template           # Connection template (copy to ~/.dbt/profiles.yml)
├── packages.yml                    # dbt packages (dbt-codegen)
├── requirements.txt                # dbt-bigquery
├── models/
│   └── firefox_desktop_derived/    # example dataset directory (mirrors bqetl)
│       ├── _firefox_desktop__sources.yml           # prod source definitions
│       ├── baseline_active_users_aggregates_v2.sql # example model
│       ├── baseline_active_users_aggregates_v2.yml # example descriptions + tests
│       ├── baseline_active_users_by_country_v1.sql # example downstream model (ref())
│       └── baseline_active_users_by_country_v1.yml # example downstream descriptions + tests
├── tests/                          # custom (singular) data tests
├── macros/
└── analyses/                       # SQL for shared analyses

```

## Getting started

### 1. Prerequisites
- `dbt-bigquery` (see below), Google Cloud SDK (`gcloud`)
- A personal BigQuery sandbox project you can write to (`moz-fx-dev-<lname>-sandbox`). Mozilla's process is [here](https://mozilla-hub.atlassian.net/wiki/spaces/SRE/pages/27924527/GCP+Access+for+Developer+Engineering+Sandbox+Project?xpis=eyJicmlkZ2UiOiJzZWFyY2hQYWdlIiwiaWQiOiIxNzg3ODU1NTIzNjA0Iiwic291cmNlIjoiY29uZmx1ZW5jZSJ9)
- Read access to source data

### 2. Install dbt and packages
Use a dedicated virtualenv (not your bigquery-etl venv)
```bash
python -m venv dbt/.venv               # dedicated dbt venv
source dbt/.venv/bin/activate
pip install -r dbt/requirements.txt    # dbt-bigquery (dbt-fusion)
cd dbt && dbt deps                     # install dbt-codegen
```

### 3. Authenticate
```bash
gcloud auth application-default login
```

### 4. Configure your profile
```bash
mkdir -p ~/.dbt
cp dbt/profiles.yml.template ~/.dbt/profiles.yml
# then edit ~/.dbt/profiles.yml and replace <lname> with your sandbox project name
```
If you already have a `~/.dbt/profiles.yml`, copy just the `dbt_dev:` block into it.

### 5. Verify
```bash
cd dbt
dbt debug        # expect "All checks passed!"
```
## Writing models

### 1. Create the model SQL

Add `models/<dataset>/<model>_vN.sql` (create the dataset directory if needed).

Use the macro `{{ submission_date_filter() }}` to limit data to one day

Use `{{ source('<dataset>', '<table>') }}` / `{{ ref('<model>') }}` for upstreams instead of hardcoded table names. For example:

```sql
-- models/firefox_desktop_derived/my_new_model_v1.sql
SELECT
  submission_date,
  -- dimension and metric columns
FROM
  {{ source('firefox_desktop', 'baseline_active_users') }}
WHERE
  -- filters to one day of data unless provided a date on model run
  {{ submission_date_filter() }}
```

### 2. Generate the YAML

Build the model, then use dbt-codegen to scaffold the schema `.yml` — see
[Generating YAML from SQL with dbt-codegen](#generating-yaml-from-sql-with-dbt-codegen)
below. Add descriptions and tests by hand.

## Running models

```bash
cd dbt

# Build and test specific model for one day (override the submission date as needed)
dbt build --select baseline_active_users_aggregates_v2 --vars '{"submission_date": "2026-08-20"}'

# Run tests (generic YAML tests + custom SQL tests)
dbt test --select baseline_active_users_aggregates_v2

# Build and test one model and everything downstream of it
dbt build --select baseline_active_users_aggregates_v2+
```

Example models are created in `moz-fx-dev-<lname>-sandbox.firefox_desktop_derived`

## Generating YAML from SQL with dbt-codegen

[dbt-codegen](https://github.com/dbt-labs/dbt-codegen) (installed via `dbt deps`, and
fusion-compatible) scaffolds boilerplate so you don't hand-write YAML. It introspects the
warehouse, so the model must be **built first**.

```bash
# 1. Build the model so codegen can read its columns
dbt run --select my_new_model_v1

# 2. Generate the schema YAML
dbt --quiet run-operation generate_model_yaml --args '{"model_names": ["my_new_model_v1"]}'
```

## Testing

- **Generic tests** are declared in the model `.yml` files (e.g. `not_null` on
  `submission_date`, `accepted_values` on `channel`).
- **Custom (singular) tests** live in `tests/` as SQL that returns failing rows; a test
  passes when it returns zero rows:
  - `assert_dau_not_greater_than_mau.sql` - enforces `dau <= mau`.
  - `assert_by_country_grain_unique.sql` - enforces one row per
    `(submission_date, country, channel)` without needing `dbt_utils`.

Other useful codegen generators:

```bash
# Scaffold a sources: block for raw tables in a dataset
dbt run-operation generate_source \
  --args '{"schema_name": "firefox_desktop", "database_name": "moz-fx-data-shared-prod", "table_names": ["baseline_active_users"]}'

# Scaffold a staging-style SELECT from a source
dbt run-operation generate_base_model \
  --args '{"source_name": "firefox_desktop", "table_name": "baseline_active_users"}'
```

codegen only prints to the console — you copy the output into files. It gets you the
column list and types; you add descriptions and tests.

## Resources
- [dbt documentation](https://docs.getdbt.com/)
- [BigQuery profile setup](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile)
- [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
