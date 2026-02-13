"""Generate metric aggregates for shared Rust components."""

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import ClassVar

import click

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.util.common import render, write_sql

THIS_PATH = Path(os.path.dirname(__file__))

def all_metric_groups() -> list["MetricGroup"]:
    """Glean metrics to aggregate / optimize"""

    return [
        MetricGroup(
            ping="metrics",
            category="logins_store",
            applications=[
                Application.firefox_android,
                Application.firefox_ios,
            ],
            metrics=[
                Counter("local_undecryptable_deleted"),
                Counter("mirror_undecryptable_deleted"),
                Event("key_regenerated_corrupt"),
                Event("key_regenerated_lost"),
                Event("key_regenerated_other"),
            ],
        ),
        MetricGroup(
            ping="metrics",
            category="places_manager",
            applications=[
                Application.firefox_android,
            ],
            metrics=[
                Distribution("db_size_after_maintenance", DistributionType.memory),
                Distribution("run_maintenance_chk_pnt_time", DistributionType.timing),
                Distribution("run_maintenance_optimize_time", DistributionType.timing),
                Distribution("run_maintenance_prune_time", DistributionType.timing),
                Distribution("run_maintenance_time", DistributionType.timing),
                Distribution("run_maintenance_vacuum_time", DistributionType.timing),
            ],
        ),
        MetricGroup(
            ping="metrics",
            category="suggest",
            applications=[
                Application.firefox_desktop,
            ],
            metrics=[
                LabeledDistribution("ingest_download_time", DistributionType.timing),
                LabeledDistribution("ingest_time", DistributionType.timing),
                LabeledDistribution("query_time", DistributionType.timing),
            ],
        ),
    ]

class Application(Enum):
    """Datasets for each application."""

    firefox_desktop = "firefox_desktop"
    firefox_android = "fenix"
    firefox_ios = "firefox_ios"

class DistributionType(Enum):
    """Glean Distribution type."""

    timing = auto()
    memory = auto()
    custom = auto()

@dataclass
class Metric:
    """Base class for metrics that we collect."""
    name: str
    template_dir: ClassVar[Path]

@dataclass
class Counter(Metric):
    template_dir = Path("counter")

@dataclass
class Distribution(Metric):
    template_dir = Path("distribution")
    type: DistributionType

@dataclass
class LabeledDistribution(Metric):
    template_dir = Path("labeled-distribution")
    type:  DistributionType

@dataclass
class Event(Metric):
    template_dir = Path("event")

def get_metric_data(metric: Metric) -> dict[str, str]:
    data = {
        "name": metric.name
    }
    match metric:
        case Distribution(_, type) | LabeledDistribution(_, type):
            table_prefix = ""
            if isinstance(metric, LabeledDistribution):
                table_prefix = "labeled_"
            match type:
                case DistributionType.timing:
                    data["table"] = f"{table_prefix}timing_distribution"
                    data["unit"] = "nanoseconds"
                case DistributionType.memory:
                    data["table"] = f"{table_prefix}memory_distribution"
                    data["unit"] = "bytes"
                case DistributionType.custom:
                    data["table"] = f"{table_prefix}custom_distribution"
                    data["unit"] = ""
    return data

@dataclass
class MetricGroup:
    """
    Group of metrics to aggregate.

    This normally corresponds to a top-level key in the `metrics.yaml` file for a Rust component.
    """

    ping: str
    """Name of the Glean ping that contains metrics for this component."""

    category: str
    """Metric category, this is the top-level key in the `metrics.yaml` file."""

    applications: list[Application]
    """Applications that collect these metrics"""

    metrics: list[Metric]
    """Metrics to aggregate in the derived dataset"""

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
    """Generate per-app queries, views and metadata for urlbar events aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """
    output_dir = Path(output_dir) / target_project

    for metric_group in all_metric_groups():
        for metric in metric_group.metrics:
            full_metric_id = f"{metric_group.ping}_{metric_group.category}_{metric.name}"
            full_table_id = f"{target_project}.rust_components_derived.{full_metric_id}_v1"
            full_view_id = f"{target_project}.rust_components.{full_metric_id}"
            metric_data = get_metric_data(metric)

            query_sql_parts = [
                render(
                    f"{metric.template_dir}/query.sql",
                    template_folder=str(THIS_PATH / "templates"),
                    application=application.name,
                    dataset_name=application.value,
                    ping=metric_group.ping,
                    category=metric_group.category,
                    metric=metric_data,
                    format=True,
                )
                for application in metric_group.applications
            ]

            write_sql(
                output_dir=output_dir,
                full_table_id=full_table_id,
                basename="query.sql",
                sql="\nUNION ALL\n".join(query_sql_parts),
                skip_existing=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=full_table_id,
                basename="metadata.yaml",
                sql=render(
                    f"metadata.yaml",
                    template_folder=str(THIS_PATH / "templates"),
                    ping=metric_group.ping,
                    category=metric_group.category,
                    metric=metric_data,
                    format=False,
                ),
                skip_existing=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=full_table_id,
                basename="schema.yaml",
                sql=render(
                    f"{metric.template_dir}/schema.yaml",
                    template_folder=str(THIS_PATH / "templates"),
                    metric=metric_data,
                    format=False,
                ),
                skip_existing=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=full_view_id,
                basename="view.sql",
                sql=render(
                    f"view.sql",
                    template_folder=str(THIS_PATH / "templates"),
                    full_view_id=full_view_id,
                    full_table_id=full_table_id,
                    format=True,
                ),
                skip_existing=False,
            )
