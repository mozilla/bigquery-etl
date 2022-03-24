"""Country code normalization UDF generation."""
import os
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

FILE_PATH = Path(os.path.dirname(__file__))
TEMPLATES_PATH = FILE_PATH / "templates"


@click.command("generate")
@click.option(
    "--target_project",
    "--target-project",
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
@click.option(
    "--parallelism",
    "-P",
    help="Maximum number of tasks to execute concurrently",
    default=20,
    type=int,
)
def generate(target_project, output_dir, parallelism):
    """
    Generate a UDF SQL file that maps country aliases to a country code.

    This script uses a template for the UDF and a
    static list of aliases for each country code.
    """
    udf_target_path = Path(f"{output_dir}/{target_project}/norm/country_code/")

    env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
    udf_template = env.get_template("udf.sql")
    metadata_template = env.get_template("metadata.yaml")

    alias_path = FILE_PATH / "aliases.yaml"
    aliases_yaml = yaml.safe_load(alias_path.read_text())

    code_to_aliases = {
        country_code: [f'"{country_code}"'] + [f'"{alias}"' for alias in aliases]
        for country_code, aliases in aliases_yaml.items()
    }

    with open(udf_target_path / "udf.sql", "w") as udf_sql_file:
        udf_sql_file.write(udf_template.render(code_to_aliases=code_to_aliases))

    with open(udf_target_path / "metadata.yaml", "w") as udf_metadata_file:
        udf_metadata_file.write(metadata_template.render())
