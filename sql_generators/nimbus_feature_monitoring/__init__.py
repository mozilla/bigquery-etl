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
    field: None | str
    aggregator: str = "last"
    value_in: None | list[Any] = None
    value_default: Any = None
    omit_default_value: bool = False


@dataclass
class SourceTable:
    name: str
    project: str
    dataset: str
    table_name: str
    dimensions: list[Dimension]
    unique_id: str = "client_info.client_id"
    date_partition_field: str = "submission_timestamp"

    @property
    def sql_table_name(self):
        return f"{self.project}.{self.dataset}.{self.table_name}"

    @property
    def date_filter(self):
        return f"CAST({self.date_partition_field} AS DATE) = @submission_date"

    @property
    def sort_field(self):
        return self.timestamp_field or self.date_field

    @property
    def dimension_aggregators(self):
        return list({d.aggregator for d in self.dimensions})


@dataclass
class Metric:
    name: str
    type_: str
    field: None | str = None
    aggregators: list[str]|tuple[str, ...] = ("sum",)
    ping_aggregator: str = "sum"
    label_aggregator: None | str = None
    label_in: None | list[str] = None


@dataclass
class Feature:
    name: str
    project: str
    dataset: str
    metrics: dict[str, list[Metric]]
    ratios: list[list[str, str]]


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
        source_tables = [
            SourceTable(
                name=source_name,
                project=source.pop("project", project),
                dataset=source.pop("dataset", dataset),
                dimensions=[
                    Dimension(
                        name=dim_name,
                        field=dim.pop("field", dim_name),
                        **dim,
                    )
                    for dim_name, dim in source.pop("dimensions", {}).items()
                ],
                **source,
            )
            for source_name, source in app_config["source_tables"].items()
        ]
        features = [
            Feature(
                name=feat_name,
                project=feat.pop("project", project),
                dataset=feat.pop("dataset", f"{dataset}_derived"),
                metrics={
                    source_name: [
                        Metric(
                            name=metric_name,
                            type_=type_,
                            field=metric.pop("field", f"metrics.{type_}.{metric_name}"),
                            **metric,
                        )
                        for type_, metrics in metrics_by_label.items()
                        for metric_name, metric in metrics.items()
                    ]
                    for source_name, metrics_by_label in feat.pop("metrics", {}).items()
                },
                ratios=feat.pop("ratios", []),
                **feat,
            )
            for feat_name, feat in app_config["features"].items()
        ]

        feature_tables = []
        for feature in features:
            args = {
                "source_tables": source_tables,
                "feature": feature,
            }

            feature_name_sql = feature.name.replace("-", "_")
            table = f"nimbus_feature_monitoring_{feature_name_sql}_v1"
            sql_table_name = f"{feature.project}.{feature.dataset}_derived.{table}"

            write_sql(
                write_dir / project,
                sql_table_name,
                "query.sql",
                reformat(sql_template.render(**args)),
            )
            write_path = Path(write_dir) / project / (dataset + "_derived") / table
            (write_path / "metadata.yaml").write_text(metadata_template.render(**args))
            (write_path / "schema.yaml").write_text(schema)

            feature_tables.append((feature.name, sql_table_name))

        # generate view over feature tables
        view_args = {
            "feature_tables": feature_tables,
            "view": f"{project}.{dataset}.nimbus_feature_monitoring",
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
