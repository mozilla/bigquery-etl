from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import Schema
from bigquery_etl.util.common import write_sql

CRASH_TABLES = [
    ("moz-fx-data-shared-prod", "firefox_desktop", "crash"),
    ("moz-fx-data-shared-prod", "firefox_crashreporter", "crash"),
]


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the queries should be written to.",
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
    schemas = {
        f"{project}.{dataset}.{table}": Schema.for_table(
            project=project,
            dataset=dataset,
            table=table,
            partitioned_by="submission_timestamp",
            use_cloud_function=use_cloud_function,
        )
        for project, dataset, table in CRASH_TABLES
    }

    combined_schema = Schema.empty()
    for table_name, schema in schemas.items():
        if len(schema.schema["fields"]) == 0:
            raise ValueError(
                f"Could not get schema for {table_name} from dry run, "
                f"possible authentication issue."
            )
        combined_schema.merge(schema)

    fields_per_table = {
        table_name: schema.generate_compatible_select_expression(
            combined_schema,
            unnest_structs=False,
        )
        for table_name, schema in schemas.items()
    }

    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(template_dir))

    template = env.get_template("desktop_crashes.view.sql")
    query = template.render(tables=fields_per_table, project_id=target_project)

    write_sql(
        Path(output_dir) / target_project,
        f"{target_project}.firefox_desktop.desktop_crashes",
        "view.sql",
        reformat(query),
    )


if __name__ == "__main__":
    generate()
