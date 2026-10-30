"""
Generate MDN legacy views that union Yari pings with reshaped Fred events.

Both sides of the UNION ALL select every field of the Yari schema explicitly,
setting fields that Fred doesn't provide to NULL. Fields added upstream by Glean
therefore can't break the deployed views, and they are picked up on the next
regeneration.
"""

import os
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import DryRun
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import Schema
from bigquery_etl.util.common import write_sql

ENV = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))

# view name -> (Yari table, Fred event name)
VIEWS = {
    "legacy_action": ("action", "element_click"),
    "legacy_page": ("page", "page_load"),
}


def fred_query(view_name, project_id):
    """Return the query that reshapes Fred events to the Yari schema."""
    _, event_name = VIEWS[view_name]
    return ENV.get_template("fred_events.query.sql").render(
        project_id=project_id, event_name=event_name
    )


def generate_view(view_name, project_id, yari_schema, fred_schema):
    """Return the SQL of a legacy view, given the Yari and Fred query schemas."""
    yari_table, _ = VIEWS[view_name]
    fred_select, yari_select = (
        schema.generate_compatible_select_expression(
            yari_schema, unnest_structs=True, retain_null_structs=True
        )
        for schema in (fred_schema, yari_schema)
    )
    return reformat(
        ENV.get_template("legacy_view.view.sql").render(
            project_id=project_id,
            view_name=view_name,
            yari_table=yari_table,
            fred_query=fred_query(view_name, project_id),
            fred_select=fred_select,
            yari_select=yari_select,
        )
    )


def _query_schema(query, project_id, use_cloud_function):
    """Return the schema of a query on tables partitioned by submission_timestamp."""
    schema = DryRun(
        os.path.join(
            ConfigLoader.get("default", "sql_dir"),
            project_id,
            "mdn",
            "query",
            "query.sql",
        ),
        content=f"SELECT * FROM ({query}) "
        "WHERE DATE(submission_timestamp) = '2020-01-01'",
        use_cloud_function=use_cloud_function,
    ).get_schema()
    if not schema.get("fields"):
        raise ValueError(f"Could not get schema from dry run of: {query}")
    return Schema(schema)


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the views should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
def generate(target_project, output_dir, use_cloud_function):
    """Generate the mdn.legacy_action and mdn.legacy_page views."""
    for view_name, (yari_table, _) in VIEWS.items():
        yari_schema = _query_schema(
            f"SELECT * FROM `{target_project}.mdn_yari.{yari_table}`",
            target_project,
            use_cloud_function,
        )
        fred_schema = _query_schema(
            fred_query(view_name, target_project), target_project, use_cloud_function
        )
        sql_dir = Path(output_dir) / target_project
        full_view_id = f"{target_project}.mdn.{view_name}"
        write_sql(
            sql_dir,
            full_view_id,
            "view.sql",
            generate_view(view_name, target_project, yari_schema, fred_schema),
        )
        write_sql(
            sql_dir,
            full_view_id,
            "metadata.yaml",
            ENV.get_template(f"{view_name}.metadata.yaml").render(),
        )
