"""Experiment monitoring materialized view generation."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql


@dataclass
class Dimension:
    name: str
    source_table: str
    source_field: None|str = None
    value_in: None|list[Any] = None
    value_default: Any = None
    aggregator: str = "last"

    def __post_init__(self):
        if self.source_field is None:
            self.source_field = self.name


@dataclass
class Metric:
    name: str
    client_aggregators: list[str]
    ping_aggregator: str
    source_table: str
    source_field: None|str = None
    label_aggregator: None|str = None
    label_in: None|list[str] = None

    def __post_init__(self):
        if self.source_field is None:
            self.source_field = self.name


@dataclass
class SourceTable:
    name: str
    project: str
    date_field: None|str = None
    timestamp_field: None|str = None

    def __post_init__(self):
        if not self.date_field and not self.timestamp_field:
            raise KeyError("source table missing date or timestamp field")

    @property
    def sql_name(self):
        if self.name.count(".") == 1:
            return f"`{self.project}.{self.name}`"
        return self.name

    @property
    def date_filter(self):
        if self.date_field:
            return f"{self.date_field} = @submission_date"
        return f"DATE({self.timestamp_field}) = @submission_date"

    @property
    def sort_field(self):
        return self.timestamp_field or self.date_field


def generate_queries(project, path, write_dir):
    """Generate nimbus feature monitoring queries."""
    env = Environment(
        loader=FileSystemLoader(template_dir := path / "nimbus_feature_monitoring_v1"),
        keep_trailing_newline=True,
    )
    sql_template = env.get_template("query.sql")
    metadata_template = env.get_template("metadata.yaml")
    view_template = env.get_template("view.sql")
    schema = (template_dir / "schema.yaml").read_text()
    for app_config_path in path.glob("*.yaml"):
        app_config = yaml.safe_load(app_config_path.read_text())
        dataset = app_config["dataset"]

        # generate per-feature tables
        dimensions = {
            key: Dimension(name=key, **value)
            for key, value in app_config["dimensions"].items()
        }
        source_tables = {
            key: SourceTable(name=key, project=project, **value)
            for key, value in app_config["source_tables"].items()
        }
        features = []
        for feature, feature_config in app_config["features"].items():
            feature_dimensions = {
                key: Dimension(name=key, **value)
                for key, value in feature_config.get("dimensions", {}).items()
            }
            feature_source_tables = {
                key: SourceTable(name=key, project=project, **value)
                for key, value in feature_config.get("source_tables", {}).items()
            }
            metrics = [
                Metric(name=key, **value)
                for key, value in feature_config["metrics"].items()
            ]

            args = {
                "dimensions": list({**dimensions, **feature_dimensions}.values()),
                "metrics": metrics,
                "feature": feature,
                "project": project,
                "unique_id": app_config["unique_id"],
                "ratios": feature_config.get("ratios", []),
            }
            used_source_tables = {
                dimension.source_table
                for dimension in args["dimensions"]
            } | {
                metric.source_table
                for metric in metrics
            }
            args["source_tables"] = [
                value
                for key, value in {**source_tables, **feature_source_tables}.items()
                if key in used_source_tables
            ]

            feature_name_sql = feature.replace("-", "_")
            table = f"nimbus_feature_monitoring_{feature_name_sql}_v1"
            full_table_name = f"{project}.{dataset}_derived.{table}"
            write_sql(
                write_dir / project,
                full_table_name,
                "query.sql",
                reformat(sql_template.render(**args)),
            )

            write_path = Path(write_dir) / project / (dataset + "_derived") / table
            (write_path / "metadata.yaml").write_text(
                metadata_template.render(**args)
            )

            (write_path / "schema.yaml").write_text(schema)

            features.append((feature, full_table_name))

        # generate view over feature tables
        view_args = {
            "dataset": dataset,
            "features": features,
            "view": f"{project}.{dataset}.nimbus_feature_monitoring"
        }
        write_sql(
            write_dir / project,
            view_args["view"],
            "view.sql",
            reformat(view_template.render(**view_args)),
        )


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--path",
    help="Where query directories will be searched for.",
    default="sql_generators/nimbus_feature_monitoring/templates",
    required=False,
    type=click.Path(file_okay=False),
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
def generate(target_project, path, output_dir, use_cloud_function):
    """Generate the nimbus feature monitoring views."""
    generate_queries(target_project, Path(path), Path(output_dir))
