"""Country code lookup generation."""
import os
from collections import Counter
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
def generate(target_project, output_dir):
    """Generate a CSV that maps country aliases to a country code."""
    target_path = Path(f"{output_dir}/{target_project}/static/country_names_v1/")

    env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
    csv_template = env.get_template("data.csv")

    alias_path = FILE_PATH / "aliases.yaml"
    aliases_yaml = yaml.safe_load(alias_path.read_text())

    aliases_to_code = sorted(
        [
            (f'"{alias}"', country_code)
            for country_code, aliases in aliases_yaml.items()
            for alias in aliases
        ],
        key=lambda x: x[1],
    )

    # It's important that each alias maps to only one code,
    # we don't want to introduce fan-out in downstream queries.
    counter = Counter(alias for alias, _ in aliases_to_code)
    for alias, count in counter.items():
        if count > 1:
            raise ValueError(
                f"Multiple mappings found for {alias}, please update aliases.yaml."
            )

    try:
        with open(target_path / "data.csv", "w") as data_file:
            data_file.write(csv_template.render(aliases_to_code=aliases_to_code))
    except FileNotFoundError:
        pass
