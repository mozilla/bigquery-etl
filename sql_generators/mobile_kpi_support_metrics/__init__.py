"""Generate active users aggregates per app."""

import itertools
from dataclasses import asdict, dataclass, field
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
    "engagement_clients.view.sql",
    "engagement.query.sql",
    "engagement.view.sql",
)

INSTALL_SOURCE = [
    {
        "name": "install_source",
        "type": "STRING",
        "description": "The source of a profile installation.",
    },
]

ADJUST_FIELDS = [
    {
        "name": "adjust_ad_group",
        "type": "STRING",
        "description": "Adjust Ad Group the profile is attributed to.",
    },
    {
        "name": "adjust_campaign",
        "type": "STRING",
        "description": "Adjust Campaign the profile is attributed to.",
    },
    {
        "name": "adjust_creative",
        "type": "STRING",
        "description": "Adjust Creative the profile is attributed to.",
    },
    {
        "name": "adjust_network",
        "type": "STRING",
        "description": "Adjust Network the profile is attributed to.",
    },
]

FENIX_ATTRIBUTION_FIELDS = [
    {
        "name": "play_store_attribution_campaign",
        "type": "STRING",
        "description": "Play store campaign the profile is attributed to.",
    },
    {
        "name": "play_store_attribution_medium",
        "type": "STRING",
        "description": "Play store Medium the profile is attributed to.",
    },
    {
        "name": "play_store_attribution_source",
        "type": "STRING",
        "description": "Play store source the profile is attributed to.",
    },
    {
        "name": "meta_attribution_app",
        "type": "STRING",
        "description": "Facebook app linked to paid marketing.",
    },
    *INSTALL_SOURCE,
    *ADJUST_FIELDS,
]

FIREFOX_IOS_ATTRIBUTION_FIELDS = [
    {
        "name": "is_suspicious_device_client",
        "type": "BOOLEAN",
        "description": "Flag to identify suspicious device users, see bug-1846554 for more info.",
    },
    *ADJUST_FIELDS,
]


@dataclass
class Product:
    """Encapsulation of what we expect a 'Product' to look like in this generator."""

    friendly_name: str
    is_mobile_kpi: bool = False
    attribution_fields: list = field(default_factory=list)


class MobileProducts(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    fenix = Product(
        friendly_name="Fenix",
        is_mobile_kpi=True,
        attribution_fields=FENIX_ATTRIBUTION_FIELDS,
    )
    focus_android = Product(
        friendly_name="Focus Android",
        is_mobile_kpi=True,
    )
    klar_android = Product(
        friendly_name="Klar Android",
    )
    firefox_ios = Product(
        friendly_name="Firefox iOS",
        is_mobile_kpi=True,
        attribution_fields=FIREFOX_IOS_ATTRIBUTION_FIELDS,
    )
    focus_ios = Product(friendly_name="Focus iOS", is_mobile_kpi=True)
    klar_ios = Product(friendly_name="Klar iOS")


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

    all_possible_attribution_fields = {
        field["name"]: field
        for field in list(
            itertools.chain.from_iterable(
                [product.value.attribution_fields for product in MobileProducts]
            )
        )
    }

    for template in TEMPLATES:
        for product in MobileProducts:
            target_name, target_filename, target_extension = template.split(".")

            target_dataset = (
                product.name + "_derived"
                if target_filename == "query"
                else product.name
            )

            table_id = f"{target_project}.{target_dataset}.{target_name}"
            full_table_id = (
                table_id + f"_{VERSION}" if target_filename == "query" else table_id
            )

            sql_template = env.get_template(template)
            rendered_sql = reformat(
                sql_template.render(
                    **asdict(product.value),
                    **default_template_args,
                    dataset=product.name,
                    target_name=target_name,
                    app_name=product.name,
                    name=target_name,
                )
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=full_table_id,
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
                    target_name=target_name,
                    app_name=product.name,
                    name=target_name,
                    format=False,
                )

                write_sql(
                    output_dir=output_dir,
                    full_table_id=full_table_id,
                    basename=query_support_config,
                    sql=(
                        reformat(support_config_rendered)
                        if query_support_config.endswith(".sql")
                        else support_config_rendered
                    ),
                    skip_existing=False,
                )

        # we only want to generate a union view inside telemetry for views
        if target_filename != "view":
            continue

        target_dataset = "telemetry"

        union_target_name = f"mobile_{target_name}"

        union_sql_template = env.get_template("union.view.sql")
        union_sql_rendered = union_sql_template.render(
            **default_template_args,
            dataset=target_dataset,
            name=target_name,
            target_name=union_target_name,
            target_filename=target_filename,
            format=False,
            products=[
                {
                    "name": product.name,
                    "all_possible_attribution_fields": (
                        [
                            {
                                "exists": field_name
                                in [
                                    field["name"]
                                    for field in product.value.attribution_fields
                                ],
                                "name": field_name,
                                "type": field_properties["type"],
                            }
                            for field_name, field_properties in all_possible_attribution_fields.items()
                        ]
                        if not template.startswith("active_users")
                        else []
                    ),
                }
                for product in MobileProducts
            ],
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{target_dataset}.{union_target_name}",
            basename=f"{target_filename}.{target_extension}",
            sql=(reformat(union_sql_rendered)),
            skip_existing=False,
        )
