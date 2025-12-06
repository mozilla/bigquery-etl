# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is Mozilla's BigQuery ETL repository containing:
- Derived ETL jobs for BigQuery
- User-defined functions (UDFs)
- Airflow DAG generation for scheduled queries
- Tools for query deployment, management, and scheduling via the `bqetl` CLI

## Setup and Installation

```bash
# Clone and install
git clone git@github.com:mozilla/bigquery-etl.git
cd bigquery-etl
./bqetl bootstrap

# Install pre-commit hooks
venv/bin/pre-commit install

# Authenticate with GCP (for Mozilla employees)
gcloud auth login --update-adc --project=moz-fx-data-shared-prod
```

Python 3.11 is required (specified in pyproject.toml).

## Common Commands

### Query Development
```bash
# Create a new query
./bqetl query create <dataset>.<table>_<version>

# Validate and format query
./bqetl query validate <dataset>.<table>_<version>

# Format SQL
./bqetl format <path/to/query.sql>

# Dry run query against BigQuery
./bqetl query dryrun <dataset>.<table>_<version>

# Update schema after query changes
./bqetl query schema update <dataset>.<table>_<version>

# Update schema and downstream dependencies
./bqetl query schema update <dataset>.<table>_<version> --update_downstream
```

### Scheduling
```bash
# List available DAGs
./bqetl dag info

# Create new DAG
./bqetl dag create <bqetl_new_dag>

# Schedule a query to a DAG
./bqetl query schedule <dataset>.<table>_<version> --dag <bqetl_dag>

# Generate DAG files from query metadata
./bqetl dag generate <bqetl_dag>
```

### Backfilling
```bash
# Backfill a query for date range
./bqetl query backfill --project-id <project_id> <dataset>.<table>_<version>
```

### UDFs and Stored Procedures
```bash
# Create mozfun UDF (public, reusable)
./bqetl mozfun create <dataset>.<name> --udf

# Create internal UDF (query-specific)
./bqetl routine create <dataset>.<name> --udf

# Create stored procedure
./bqetl routine create <dataset>.<name> --stored_procedure

# Validate UDF
./bqetl mozfun validate <dataset>.<name>
./bqetl routine validate <dataset>.<name>

# Rename UDF (automatically updates references)
./bqetl mozfun rename <dataset>.<name> <new_dataset>.<new_name>
```

### Testing
```bash
# Run all tests
pytest

# Run tests for specific query
pytest sql/moz-fx-data-shared-prod/<dataset>/<table>_<version>/

# Run specific test file
pytest tests/path/to/test_file.py
```

### Documentation
```bash
# Generate and serve docs locally
./bqetl docs generate --output_dir generated_docs
cd generated_docs
mkdocs serve
```

## Repository Structure

### SQL Organization (`sql/`)

Queries are hierarchically organized:
```
sql/
├── bigconfig.yml                          # Bigeye data quality monitoring config
├── mozfun/                                # Public UDFs (reusable functions)
│   ├── assert/
│   ├── glam/
│   ├── hist/
│   └── ...
└── moz-fx-data-shared-prod/              # Main BigQuery project
    ├── <dataset_name>/
    │   └── <table_name>_v<version>/
    │       ├── query.sql                  # Main SQL query
    │       ├── metadata.yaml              # Scheduling, BigQuery config, owners
    │       ├── schema.yaml                # Table schema (optional)
    │       ├── checks.sql                 # Data quality checks (optional)
    │       └── tests/                     # Test data and expectations
    │           ├── expect.yaml            # Expected output
    │           └── input/                 # Input test data (.ndjson)
    └── ...
```

**Key Patterns:**
- Tables are versioned (`_v1`, `_v2`) for safe schema evolution
- Datasets suffixed with `_derived` are internal; others may be user-facing
- Each query directory contains `query.sql` and `metadata.yaml` at minimum
- Views use `view.sql` instead of `query.sql`

### Python Package (`bigquery_etl/`)

Core ETL framework code:
```
bigquery_etl/
├── cli/                    # bqetl command implementations
│   ├── query.py           # Query lifecycle management
│   ├── dag.py             # DAG operations
│   ├── routine.py         # UDF/stored procedure management
│   └── ...
├── query_scheduling/       # DAG generation from metadata
│   ├── dag.py             # DAG model classes
│   ├── task.py            # Task and dependency models
│   └── generate_airflow_dags.py  # Main DAG generator
├── metadata/              # Metadata parsing and validation
├── util/                  # Common utilities
├── format_sql/            # SQL formatter
└── pytest_plugin/         # Custom pytest plugins for SQL testing
```

### SQL Generators (`sql_generators/`)

Code generators for standard query patterns:
- `stable_views` - User-facing views on raw tables
- `active_users_aggregates_v*` - Active user metrics
- `events_daily` - Daily event aggregations
- `glean_usage` - Glean-specific metrics
- `experiment_monitoring` - Experiment analysis

### Configuration Files

- `dags.yaml` - Master DAG configuration (schedule, owners, retries)
- `bqetl_project.yaml` - Project-level configuration (skip lists, test projects)
- `pyproject.toml` - Python package configuration

## Query Metadata Structure

Every query has a `metadata.yaml` file with this structure:

```yaml
friendly_name: Human Readable Name
description: Detailed description of what this query does
owners:
  - email@mozilla.com
labels:
  application: app_name
  schedule: daily
  change_controlled: true  # For change-controlled data

# Scheduling configuration
scheduling:
  dag_name: bqetl_dag_name              # Which DAG to add this to
  date_partition_parameter: submission_date
  date_partition_offset: -7             # Process data 7 days in the past
  depends_on_past: false
  task_concurrency: 1

# BigQuery table configuration
bigquery:
  time_partitioning:
    type: day
    field: submission_date
    require_partition_filter: false
  clustering:
    fields: [country, os]
```

## DAG Generation and Scheduling

The repository automatically generates Airflow DAGs from query metadata:

1. Queries define scheduling in `metadata.yaml`
2. DAG properties defined in `dags.yaml`
3. Run `./bqetl dag generate <dag_name>` to create DAG Python files
4. DAGs are deployed via `bqetl_artifact_deployment` Airflow DAG

**DAG Configuration in dags.yaml:**
```yaml
bqetl_dag_name:
  schedule_interval: "0 2 * * *"  # Cron format
  description: DAG description
  default_args:
    owner: user@mozilla.com
    start_date: "2020-01-01"
    retries: 2
    retry_delay: 30m
  tags:
    - impact/tier_1
```

## Testing Framework

Tests use custom pytest plugins that:
- Discover SQL tests based on `expect.yaml/json/ndjson` files
- Load test data from `tests/input/*.ndjson`
- Execute queries in BigQuery
- Compare results against expectations

**Test Structure:**
```
sql/.../query_name_v1/
├── query.sql
└── tests/
    ├── expect.yaml           # Expected output
    └── input/
        └── table_name.ndjson # Test input data
```

Fixtures in `conftest.py` provide `bigquery_client`, `temporary_dataset`, and `storage_client`.

## Development Workflows

### Adding a New Query

1. Create query: `./bqetl query create <dataset>.<table>_v1`
2. Write SQL in generated `query.sql` file
3. Update `metadata.yaml` with description, owners, and BigQuery config
4. Generate schema: `./bqetl query schema update <dataset>.<table>_v1`
5. Add test data and expectations in `tests/` directory
6. Validate: `./bqetl query validate <dataset>.<table>_v1`
7. Run tests: `pytest sql/.../query_name_v1/`
8. Schedule to DAG: `./bqetl query schedule <dataset>.<table>_v1 --dag <dag_name>`
9. Create PR

### Updating an Existing Query

1. Modify `query.sql`
2. Validate: `./bqetl query validate <dataset>.<table>_v1`
3. Update schema if needed: `./bqetl query schema update <dataset>.<table>_v1`
4. If scheduling changed: `./bqetl dag generate <dag_name>`
5. Create PR

### Adding a New Field to a Table

When adding fields that propagate to downstream tables:

1. Modify `query.sql` to add field
2. Format: `./bqetl format <path/to/query.sql>`
3. Validate: `./bqetl query validate <dataset>.<table>_v1`
4. Update schemas: `./bqetl query schema update <dataset>.<table>_v1 --update_downstream`
   - This automatically updates downstream dependencies
5. Create PR

### Adding a UDF

**Public (mozfun) UDF:**
```bash
./bqetl mozfun create <dataset>.<name> --udf
# Edit sql/mozfun/<dataset>/<name>/udf.sql
./bqetl mozfun validate <dataset>.<name>
```

**Internal UDF:**
```bash
./bqetl routine create <dataset>.<name> --udf
# Edit sql/moz-fx-data-shared-prod/<dataset>/<name>/udf.sql
./bqetl routine validate <dataset>.<name>
```

## Key Architecture Concepts

### Dependency Resolution

- The system uses `sqlglot` to parse SQL and extract table references
- Builds dependency graphs across all queries
- Used for task ordering in DAGs and backfill operations
- Can detect circular dependencies

### Incremental Queries

Queries typically use a date parameter for incremental processing:
- `@submission_date` or `@date` parameter in SQL
- `date_partition_parameter` in metadata specifies the parameter name
- `date_partition_offset` controls how far back to process (e.g., -7 for 7 days ago)

### Deployment

Changes deploy through the `bqetl_artifact_deployment` Airflow DAG:
- Runs nightly to deploy merged changes
- Handles table creation, schema updates, UDF publishing
- Clear the latest DAG run to deploy changes immediately

## Code Quality

### SQL Formatting

- Custom SQL formatter enforces consistent style
- Run `./bqetl format` before committing
- Integrated with pre-commit hooks
- Disable formatting with `-- format:off` and `-- format:on` comments

### Pre-commit Hooks

Installed hooks check:
- SQL formatting
- Python code style (black, flake8, isort)
- YAML validation
- Type checking (mypy)

### Change Control

For restricted datasets (see `CODEOWNERS`):
- Add `change_controlled: true` label to `metadata.yaml`
- GitHub team automatically assigned as reviewers
- Additional approval required from CODEOWNERS

## Data Quality Monitoring

Bigeye monitoring configured via `bigconfig.yml` files:
- Define metrics for tables (volume, freshness, null checks)
- Group metrics into collections
- Configure Slack notifications
- Deployed automatically via `bqetl_artifact_deployment` DAG

**Do not run `bigconfig apply` locally** - let the DAG handle deployment to avoid accidental deletions.

## Important Notes

- **Authentication required**: Most operations require GCP authentication with BigQuery access
- **Test project**: Integration tests use `bigquery-etl-integration-test` project
- **Main project**: Production queries target `moz-fx-data-shared-prod`
- **User-facing project**: Public data exposed via `mozdata` project
- **File naming**: Use `query.sql` for tables, `view.sql` for views
- **Multipart queries**: Use `part1.sql`, `part2.sql` for complex queries
- **Version suffix**: Always include version in table names (`_v1`, `_v2`)
- **Fork PRs**: Contributors should follow fork workflow; maintainers run CI via GitHub Actions

## Configuration Access

The `ConfigLoader` class provides programmatic access to `bqetl_project.yaml`:

```python
from bigquery_etl.config import ConfigLoader

cfg = ConfigLoader.get("dry_run", "skip", fallback=[])
```

## Additional Resources

- Documentation: https://mozilla.github.io/bigquery-etl/
- Telemetry docs: https://docs.telemetry.mozilla.org/
- Airflow UI: https://workflow.telemetry.mozilla.org/
