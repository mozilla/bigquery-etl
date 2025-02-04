"""Generate active users aggregates per app."""

import itertools
from collections import ChainMap
from dataclasses import asdict, dataclass, field
from enum import Enum
from os import path
from pathlib import Path
from typing import Any

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"-- Query generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"
TEMPLATES = (
    ("CLIENT", "active_users.view.sql"),
    ("CLIENT", "retention_clients.view.sql"),
    ("AGGREGATE", "retention.query.sql"),
    ("AGGREGATE", "retention.view.sql"),
    ("CLIENT", "engagement_clients.view.sql"),
    ("AGGREGATE", "engagement.query.sql"),
    ("AGGREGATE", "engagement.view.sql"),
    ("CLIENT", "attribution_clients.view.sql"),
    ("CLIENT", "attribution_clients.query.sql"),
    ("CLIENT", "new_profile_clients.view.sql"),
    ("AGGREGATE", "new_profiles.view.sql"),
    ("AGGREGATE", "new_profiles.query.sql"),
    ("CLIENT", "new_profile_activation_clients.view.sql"),
    ("CLIENT", "new_profile_activation_clients.query.sql"),
    ("AGGREGATE", "new_profile_activations.view.sql"),
    ("AGGREGATE", "new_profile_activations.query.sql"),
)
BIGEYE_COLLECTION = "Operational Checks"
BIGEYE_NOTIFICATION_SLACK_CHANNEL = "#de-bigeye-triage"


class AttributionPings(Enum):
    """An enumerator containing a list of pings that could be the source of attribution information."""

    metrics: str = "metrics"
    first_session: str = "first_session"
    baseline: str = "baseline"


@dataclass
class AttributionFieldGroup:
    """An object to encapsulate a logical grouping of attribution fields together."""

    name: str
    source_pings: list[AttributionPings]
    fields: list[dict[str, Any]]


class AttributionFields:
    """Defines all possible AttributionFieldGroups."""

    install_source = AttributionFieldGroup(
        name="install_source",
        source_pings=[AttributionPings.metrics],
        fields=[
            {
                "name": "install_source",
                "type": "STRING",
                "description": "The source of a profile installation.",
            },
        ],
    )
    adjust = AttributionFieldGroup(
        name="adjust",
        source_pings=[AttributionPings.metrics, AttributionPings.first_session],
        fields=[
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
            {
                "name": "adjust_attribution_timestamp",
                "type": "TIMESTAMP",
                "description": "Timestamp corresponding to the ping that contained the adjust attribution.",
                "client_only": True,
            },
        ],
    )
    play_store = AttributionFieldGroup(
        name="play_store",
        source_pings=[AttributionPings.first_session],
        fields=[
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
                "name": "play_store_attribution_timestamp",
                "type": "TIMESTAMP",
                "description": "Timestamp corresponding to the ping that contained the play_store attribution.",
                "client_only": True,
            },
            {
                "name": "play_store_attribution_content",
                "type": "STRING",
                "description": "",
                "client_only": True,
            },
            {
                "name": "play_store_attribution_term",
                "type": "STRING",
                "description": "",
                "client_only": True,
            },
            {
                "name": "play_store_attribution_install_referrer_response",
                "type": "STRING",
                "description": "Play store source the profile is attributed to.",
                "client_only": True,
            },
        ],
    )
    meta = AttributionFieldGroup(
        name="meta",
        source_pings=[AttributionPings.first_session],
        fields=[
            {
                "name": "meta_attribution_app",
                "type": "STRING",
                "description": "Facebook app linked to paid marketing.",
            },
            {
                "name": "meta_attribution_timestamp",
                "type": "TIMESTAMP",
                "description": "Timestamp corresponding to the ping that contained the meta attribution.",
                "client_only": True,
            },
        ],
    )
    is_suspicious_device_client = AttributionFieldGroup(
        name="is_suspicious_device_client",
        source_pings=[],
        fields=[
            {
                "name": "is_suspicious_device_client",
                "type": "BOOLEAN",
                "description": "Flag to identify suspicious device users, see bug-1846554 for more info.",
            },
        ],
    )
    distribution_id = AttributionFieldGroup(
        name="distribution_id",
        source_pings=[
            AttributionPings.first_session,
            AttributionPings.baseline,
            AttributionPings.metrics,
        ],
        fields=[
            {
                "name": "distribution_id",
                "type": "STRING",
                "description": "A string containing the distribution identifier.",
            },
        ],
    )
    empty = AttributionFieldGroup(
        name="empty",
        source_pings=[],
        fields=[],
    )


@dataclass
class Product:
    """Encapsulation of what we expect a 'Product' to look like in this generator."""

    friendly_name: str
    is_mobile_kpi: bool = False
    attribution_groups: list[AttributionFieldGroup] = field(
        default_factory=list[AttributionFields.empty]  # type: ignore[valid-type]
    )

    def get_product_attribution_fields(self) -> dict[str, dict[str, Any]]:
        """Merge all AttributionFieldGroups assigned to a product into a single map containing all attribution fields set for that product."""
        return {
            field["name"]: field
            for field in list(
                itertools.chain.from_iterable(
                    [
                        attribution_group.fields
                        for attribution_group in self.attribution_groups
                    ]
                )
            )
        }

    def get_attribution_pings(self) -> list[str]:
        """Merge all source_pings of AttributionFieldGroups assigned to \
        a product into a single list of all pings containing attribution data for that specific product."""
        return list(
            set(
                [
                    ping.value
                    for attribution_group in self.attribution_groups
                    for ping in attribution_group.source_pings
                ]
            )
        )

    def get_attribution_group_names(self) -> list[str]:
        """Return a list containing AttributionGroup names only."""
        return [attribution_group.name for attribution_group in self.attribution_groups]


class MobileProducts(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    fenix = Product(
        friendly_name="Firefox Android",
        is_mobile_kpi=True,
        attribution_groups=[
            AttributionFields.play_store,
            AttributionFields.meta,
            AttributionFields.install_source,
            AttributionFields.adjust,
            AttributionFields.distribution_id,
        ],
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
        attribution_groups=[
            AttributionFields.is_suspicious_device_client,
            AttributionFields.adjust,
        ],
    )
    focus_ios = Product(
        friendly_name="Focus iOS",
        is_mobile_kpi=True,
    )
    klar_ios = Product(
        friendly_name="Klar iOS",
    )


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
        "bigeye_collection": BIGEYE_COLLECTION,
        "bigeye_notification_slack_channel": BIGEYE_NOTIFICATION_SLACK_CHANNEL,
    }

    query_support_configs = (
        "metadata.yaml",
        "schema.yaml",
        "bigconfig.yml",
    )

    all_possible_attribution_fields = dict(
        ChainMap(
            *[
                product.value.get_product_attribution_fields()
                for product in MobileProducts
            ]
        )
    )

    for template_grain, template in TEMPLATES:
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

            product_args = {
                "dataset": product.name,
                "target_name": target_name,
                "app_name": product.name,
                "name": target_name,
                "product_attribution_groups": product.value.attribution_groups,
                "product_attribution_group_names": product.value.get_attribution_group_names(),
                "product_attribution_group_pings": product.value.get_attribution_pings(),
                "product_attribution_fields": product.value.get_product_attribution_fields(),
            }

            sql_template = env.get_template(template)
            rendered_sql = reformat(
                sql_template.render(
                    **default_template_args,
                    **asdict(product.value),
                    **product_args,
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
                    **default_template_args,
                    **asdict(product.value),
                    **product_args,
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

        # For now skipping attribution_clients union. Will add in the future.
        if target_name == "attribution_clients":
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
            template_grain=template_grain,
            format=False,
            products=[
                {
                    "name": product.name,
                    "all_possible_attribution_fields": (
                        [
                            {
                                "exists": field_name
                                in product.value.get_product_attribution_fields().keys(),
                                "name": field_name,
                                "type": field_properties["type"],
                                "client_only": field_properties.get(
                                    "client_only", False
                                ),
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
