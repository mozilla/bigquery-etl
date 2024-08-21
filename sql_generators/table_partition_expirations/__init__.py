import shutil
from pathlib import Path

import click
from jinja2 import FileSystemLoader, Environment

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from bigquery_etl.util.common import write_sql, get_table_dir


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
    stable_datasets = sorted(
        {schema.bq_dataset_family + "_stable" for schema in get_stable_table_schemas()}
    )

    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(template_dir))

    output_table = f"{target_project}.monitoring_derived.table_partition_expirations_v1"

    template = env.get_template("table_partition_expirations.sql")
    query = template.render(datasets=stable_datasets, destination_table=output_table)

    table_dir = get_table_dir(Path(output_dir) / target_project, output_table)

    write_sql(
        Path(output_dir) / target_project,
        output_table,
        "query.sql",
        reformat(query),
    )

    shutil.copyfile(
        template_dir / "schema.yaml",
        table_dir / "schema.yaml",
    )

    shutil.copyfile(
        template_dir / "metadata.yaml",
        table_dir / "metadata.yaml",
    )


if __name__ == "__main__":
    generate()
