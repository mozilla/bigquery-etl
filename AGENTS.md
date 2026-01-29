# AGENTS.md - BigQuery ETL

Mozilla's ETL infrastructure for BigQuery. Data engineers use this repo to create scheduled SQL queries that power dashboards, metrics, and data products.

## Project Structure
```
sql/<project>/<dataset>/<table_v1>/    # SQL queries (versioned tables)
  query.sql                            # Main query (Jinja2 template)
  metadata.yaml                        # Required: owners, description
  schema.yaml                          # Output schema
  checks.sql                           # Data quality checks (optional)

sql/mozfun/                            # Shared UDFs (public library)
sql_generators/                        # Auto-generated SQL templates
tests/sql/                             # Tests mirror sql/ structure
bigquery_etl/                          # Python CLI and utilities
dags.yaml                              # Airflow DAG definitions
```

## Useful Commands
- `./bqetl --help` - see all available commands
- `./bqetl query validate <dataset>.<table>` - dry run and format query
- `./bqetl format <path>` - apply SQL formatting (CI enforced)
- `pytest -k <test_name>` - run specific tests

### SQL Generation
Generate SQL files with `./bqetl generate`. When running locally, add `--output_dir=/tmp/sql_test_{suffix}` flag to avoid writing to `sql` directory directly. Omit or specify `{suffix}` as needed, for example if comparing different runs.

## Documentation
- Creating queries: `docs/cookbooks/creating_a_derived_dataset.md`
- Common workflows: `docs/cookbooks/common_workflows.md`
- Testing guide: `docs/cookbooks/testing.md`
- Reference docs: `docs/reference/`

## Workflow
- Do not perform commits yourself, ever
