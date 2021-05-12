"""Generating and run baseline_clients_daily queries for Glean apps."""
from google.cloud import bigquery

from bigquery_etl.glean_usage.common import (
    generate_query,
    render,
    table_names_from_baseline,
    write_sql,
)

BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
QUERY_FILENAME = f"{BASELINE_DAILY_TABLE_ID}.sql"
VIEW_FILENAME = f"{BASELINE_DAILY_TABLE_ID[:-3]}.view.sql"
VIEW_METADATA_FILENAME = f"{BASELINE_DAILY_TABLE_ID[:-3]}.metadata.yaml"


def main():
    """Generate and run queries based on CLI args."""
    generate_query(generate, __doc__)


def generate(project_id, baseline_table, output_dir=None, output_only=False):
    """Generate the baseline table query."""
    tables = table_names_from_baseline(baseline_table, include_project_id=False)

    daily_table = tables["daily_table"]
    daily_view = tables["daily_view"]
    render_kwargs = dict(
        header="-- Generated via bigquery_etl.glean_usage\n", project_id=project_id
    )
    render_kwargs.update(tables)

    query_sql = render(QUERY_FILENAME, **render_kwargs)
    init_sql = render(QUERY_FILENAME, init=True, **render_kwargs)
    view_sql = render(VIEW_FILENAME, **render_kwargs)
    view_metadata = render(VIEW_METADATA_FILENAME, format=False, **render_kwargs)

    if output_dir:
        write_sql(output_dir, daily_view, "metadata.yaml", view_metadata)
        write_sql(output_dir, daily_view, "view.sql", view_sql)
        write_sql(output_dir, daily_table, "query.sql", query_sql)
        write_sql(output_dir, daily_table, "init.sql", init_sql)

    if output_only:
        return

    # dry run generated query
    client = bigquery.Client(project_id)
    job_kwargs = dict(use_legacy_sql=False, dry_run=True)
    job_config = bigquery.QueryJobConfig(**job_kwargs)
    client.query(query_sql, job_config)


if __name__ == "__main__":
    main()
