"""Generate authorized views in fxa_fastly_logs for Fastly CDN logs."""

from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(__file__).parent
TEMPLATES_PATH = FILE_PATH / "templates"
DATASET = "fxa_fastly_logs"

SERVICES: list[str] = [
    "accounts",
    "api_accounts",
    "eventbroker",
    "oauth",
    "payments",
    "payments_api",
    "payments_next",
    "profile",
]
# (env, stage) pairs matching the upstream Fastly dataset naming convention
# `fxa_<service>_<env>_<stage>_fastly_cdn_logs`. `env` is the deployment env;
# `stage` is the application tier within that env
ENVS: list[tuple[str, str]] = [
    ("prod", "prod"),
    ("nonprod", "stage"),
]


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
    """Generate authorized views for each service/env combination."""
    jinja_env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
    view_template = jinja_env.get_template("view.sql")
    metadata_template = jinja_env.get_template("metadata.yaml")

    output_dir = Path(output_dir)

    for service in SERVICES:
        for env, stage in ENVS:
            view_name = f"{env}_{service}"
            full_table_id = f"{target_project}.{DATASET}.{view_name}"

            view_sql = view_template.render(
                target_project=target_project,
                dataset=DATASET,
                service=service,
                env=env,
                stage=stage,
            )
            write_sql(
                output_dir / target_project,
                full_table_id,
                "view.sql",
                reformat(view_sql),
            )

            metadata = metadata_template.render(service=service, env=env, stage=stage)
            metadata_path = (
                output_dir / target_project / DATASET / view_name / "metadata.yaml"
            )
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata_path.write_text(metadata)


if __name__ == "__main__":
    generate()
