"""Usage Reporting ETL."""

import os
from functools import partial
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

GENERATOR_ROOT = Path(os.path.dirname(__file__))

HEADER = f"Generated via `{GENERATOR_ROOT.name}` SQL generator."

TEMPLATES_DIRECTORY = "templates"

# List of required artifacts by each template in this generator
TEMPLATE_ARTIFACTS = (
    "metadata.yaml.jinja",
    "schema.yaml.jinja",
    "bigconfig.yml.jinja",
    "query.sql.jinja",
)
OPTIONAL_VIEW_ARTIFACT = "view.sql.jinja"


def get_generation_config():
    """Retrieve external configuration for this generator."""
    # TODO: maybe we should have a data structure defined for this config and
    #       do validation as part of it.
    return ConfigLoader.get("generate", "terms_of_use", fallback=[])


def render_and_write_to_file(
    jinja_env: Environment,
    output_dir: Path,
    template: str,
    template_args: Dict[str, Any],
    table_id: str,
    basename: str,
    format: bool = True,
) -> None:
    """Render a Jinja template and write it to file."""
    rendered = jinja_env.get_template(template).render(**template_args)

    write_sql(
        output_dir=output_dir,
        skip_existing=False,
        full_table_id=table_id,
        basename=basename,
        sql=reformat(rendered) if format else rendered,
    )
    return None


def generate_terms_of_use(target_project: str, output_dir: Path):
    """Generate usage_reporting queries and views."""
    terms_of_use_config = get_generation_config()
    bigeye_defaults = terms_of_use_config["bigeye_defaults"]
    terms_of_use_apps = terms_of_use_config["apps"]
    dag_name = terms_of_use_config["dag_name"]

    output_dir = Path(output_dir) / target_project
    jinja_env = Environment(
        loader=FileSystemLoader(str(GENERATOR_ROOT / TEMPLATES_DIRECTORY))
    )

    render_and_write_to_file_partial = partial(
        render_and_write_to_file, jinja_env=jinja_env, output_dir=output_dir
    )

    for app_name, app_config in terms_of_use_apps.items():
        app_args = {
            "header": HEADER,
            "dag_name": dag_name,
            "project_id": target_project,
            "bigeye_collection": app_config.get("bigeye", dict()).get("collection")
            or bigeye_defaults["collection"],
            "slack_notification_channel": app_config.get("bigeye", dict()).get(
                "notification_channel"
            )
            or bigeye_defaults["notification_channel"],
            "app_name": app_name,
        }

        for template in app_config["templates"]:
            template_args = {
                **app_args,
                "table_name": template,
                "table_name_no_version": "_".join(template.split("_")[:-1]),
            }

            for artifact in TEMPLATE_ARTIFACTS:
                artifact_name, artifact_type, _ = artifact.split(".")

                template_path = f"{template}/{artifact}"
                table_id = f"{target_project}.{app_name}_derived.{template}"

                render_and_write_to_file_partial(
                    template=template_path,
                    template_args=template_args,
                    table_id=table_id,
                    basename=f"{artifact_name}.{artifact_type}",
                    format=True if artifact_type == "sql" else False,
                )

            # If view template also defined then render and write it
            if os.path.exists(
                f"{GENERATOR_ROOT}/{TEMPLATES_DIRECTORY}/{template}/{OPTIONAL_VIEW_ARTIFACT}"
            ):
                view_id = f"{target_project}.{app_name}.{template_args['table_name_no_version']}"
                render_and_write_to_file_partial(
                    template=f"{template}/{OPTIONAL_VIEW_ARTIFACT}",
                    template_args=template_args,
                    table_id=view_id,
                    basename="view.sql",
                    format=True,
                )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--output_dir", default="sql")
    args = parser.parse_args()

    generate_terms_of_use(args.project, args.output_dir)
