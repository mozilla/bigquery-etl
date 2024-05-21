"""Generate active users aggregates per app."""

from dataclasses import asdict, dataclass
from enum import Enum
from os import path
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"-- Query generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"
TEMPLATES = (
    "active_users.view.sql",
    "retention_clients.view.sql",
    "retention.query.sql",
    "retention.view.sql",
)


@dataclass
class Product:
    """Encapsulation of what we expect a 'Product' to look like in this generator."""

    friendly_name: str
    is_mobile_kpi: bool = False
    is_desktop_kpi: bool = False
    has_mozilla_online: bool = False
    baseline_view_only: bool = (
        False  # TODO: for now only fenix and firefox_ios has a client table
    )


class MobileProducts(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    fenix = Product(friendly_name="Fenix", is_mobile_kpi=True, has_mozilla_online=True)
    focus_android = Product(
        friendly_name="Focus Android", is_mobile_kpi=True, baseline_view_only=True
    )
    firefox_ios = Product(friendly_name="Firefox iOS", is_mobile_kpi=True)
    focus_ios = Product(
        friendly_name="Focus iOS", is_mobile_kpi=True, baseline_view_only=True
    )
    klar_ios = Product(friendly_name="Klar iOS", baseline_view_only=True)
    klar_android = Product(friendly_name="Klar Android", baseline_view_only=True)


@click.command()
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--target-project",
    "--target_project",
    help="Google Cloud project ID",
    default="moz-fx-data-shared-prod",
)
@use_cloud_function_option
def generate(target_project, output_dir, use_cloud_function):
    """Generate per-app queries, views and metadata for active users and search counts aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """
    env = Environment(loader=FileSystemLoader(str(GENERATOR_ROOT / "templates")))
    output_dir = Path(output_dir) / target_project

    default_template_args = {
        "header": HEADER,
        "version": VERSION,
        "project_id": target_project,
    }

    query_support_configs = (
        "checks.sql",
        "metadata.yaml",
        "schema.yaml",
    )

    for product in MobileProducts:
        for template in TEMPLATES:
            target_name, target_filename, target_extension = template.split(".")
            target_dataset = (
                product.name + "_derived"
                if target_filename == "query"
                else product.name
            )

            # Other SQL requires a clients table, currently only fenix and firefox_ios has one.
            # This is why skipping this skip for now.
            if product.value.baseline_view_only and not target_name.startswith(
                "active_users"
            ):
                continue

            sql_template = env.get_template(template)
            rendered_sql = reformat(
                sql_template.render(
                    **asdict(product.value),
                    **default_template_args,
                    dataset=target_dataset,
                    name=target_name,
                )
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{target_dataset}.{target_name}",
                basename=f"{target_filename}.{target_extension}",
                sql=rendered_sql,
                skip_existing=False,
            )

            # we only want to copy files defined in query_support_configs for query files.
            if target_filename != "query":
                continue

            for query_support_config in query_support_configs:
                support_config_template = env.get_template(
                    f"{target_name}.{query_support_config}"
                )
                support_config_rendered = support_config_template.render(
                    **asdict(product.value),
                    **default_template_args,
                    dataset=target_dataset,
                    name=target_name,
                    format=False,
                )

                write_sql(
                    output_dir=output_dir,
                    full_table_id=f"{target_project}.{target_dataset}.{target_name}",
                    basename=query_support_config,
                    sql=(
                        reformat(support_config_rendered)
                        if query_support_config.endswith(".sql")
                        else support_config_rendered
                    ),
                    skip_existing=False,
                )
