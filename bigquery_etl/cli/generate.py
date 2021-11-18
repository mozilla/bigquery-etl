"""bigquery-etl CLI generate definitions from source code."""

import os
import shutil
import yaml
from pathlib import Path

import click
import jinja2

VIEW_TEMPLATE = """CREATE OR REPLACE VIEW
  `{{ view_name }}`
AS
SELECT
  *
FROM
  `{{ source_name }}`

"""


def _get_path_tuples(path):
    """Small helper function to get base path names"""
    return map(lambda p: (p.name, p), path.iterdir())


@click.command(help="Generate SQL from definitions")
@click.argument(
    "definition_paths", type=click.Path(file_okay=True, path_type=Path), nargs=-1
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False, path_type=Path),
    default="sql",
)
def generate(definition_paths, output_dir):
    if not definition_paths:
        definition_paths = [Path("definitions")]
    for definition_path in definition_paths:
        for (project, project_path) in _get_path_tuples(definition_path):
            for (namespace, namespace_path) in _get_path_tuples(project_path):
                base_output_dir = output_dir / project
                for (table, table_path) in _get_path_tuples(namespace_path):
                    table_metadata = yaml.safe_load(
                        open(table_path / "metadata.yaml").read()
                    )
                    table = table_metadata.get("table", {}).get("name") or table
                    table_version = table_metadata.get("version", "1")
                    source_name = (
                        f"{project}.{namespace}_derived.{table}_v{table_version}"
                    )

                    #
                    # output table
                    #
                    table_output_dir = (
                        base_output_dir
                        / f"{namespace}_derived"
                        / f"{table}_v{table_version}"
                    )
                    os.makedirs(table_output_dir, exist_ok=True)
                    shutil.copyfile(
                        table_path / "query.sql", table_output_dir / "query.sql"
                    )
                    # FIXME: copy init.sql (if it exists)
                    # merge table-specific metadata with dataset metadata
                    open(table_output_dir / "metadata.yaml", "w").write(
                        yaml.dump(
                            {
                                **{
                                    k: table_metadata[k]
                                    for k in ["friendly_name", "description", "owners"]
                                },
                                **table_metadata.get("table", {}),
                            }
                        )
                    )

                    #
                    # output views
                    #
                    base_view_output_dir = base_output_dir / namespace

                    # we support defining custom versioned views for datasets
                    # if one exists, we will version it the same as the table
                    custom_view_filename = table_path / "view.sql"
                    if custom_view_filename.exists():
                        source_name = f"{project}.{namespace}.{table}_v{table_version}"
                        view_output_dir = (
                            base_view_output_dir / f"{table}_v{table_version}"
                        )
                        os.makedirs(view_output_dir, exist_ok=True)
                        shutil.copyfile(
                            custom_view_filename, view_output_dir / "view.sql"
                        )

                    # finally, create a view pointing at either the latest view (if it exists),
                    # or the latest table
                    # FIXME: Document this behaviour and assumptions in the specification
                    main_view_output_dir = base_view_output_dir / table
                    os.makedirs(main_view_output_dir, exist_ok=True)
                    open(main_view_output_dir / "view.sql", "w").write(
                        jinja2.Template(VIEW_TEMPLATE).render(
                            source_name=source_name,
                            view_name=f"{project}.{namespace}.{table}",
                        )
                    )
