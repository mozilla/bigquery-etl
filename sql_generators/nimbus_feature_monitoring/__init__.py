"""Experiment monitoring materialized view generation."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
from metric_config_parser.featmon import FeatmonSpec
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql


def value_as_sql(value):
    return "NULL" if value is None else f"{value!r}"


@dataclass
class Dimension:
    name: str
    field: None | str
    aggregator: str = "last"
    value_in: None | list[Any] = None
    # only used with value_in
    default_value: Any = None

    def __post_init__(self):
        # defined here so it can be used in multiple places in the query template
        self.value_expression = self.field
        if self.value_in:
            value_in = ",".join(value_as_sql(e) for e in self.value_in)
            self.value_expression = (
                f"IF({self.value_expression} IN ({value_in}), {self.value_expression},"
                f"{value_as_sql(self.default_value)})"
            )


@dataclass
class SourceTable:
    name: str
    project: str
    dataset: str
    table_name: str
    dimensions: list[Dimension]
    analysis_unit_id: str = "client_info.client_id"
    time_partition_field: str = "submission_timestamp"
    type: str = "metrics"

    def __post_init__(self):
        self.last_dimensions = [d for d in self.dimensions if d.aggregator == "last"]
        self.non_last_dimensions = [
            d for d in self.dimensions if d.aggregator != "last"
        ]


@dataclass
class Metric:
    name: str
    data_type: str
    field: None | str
    aggregators: list[str]
    ping_aggregator: str

    def __post_init__(self):
        self.is_labeled = isinstance(self, LabeledMetric)

    @classmethod
    def from_(_, source_type="metrics", **kwargs) -> "Metric":
        data_type = kwargs["data_type"]

        if (_key := "field") not in kwargs:
            if data_type == "event":
                kwargs[_key] = "events"
            else:
                kwargs[_key] = f"metrics.{data_type}.{kwargs['name']}"

        if (_key := "aggregators") not in kwargs:
            if data_type in ("boolean", "labeled_boolean"):
                kwargs[_key] = ["count_true"]
            else:
                kwargs[_key] = ["avg"]

        if (_key := "ping_aggregator") not in kwargs:
            if data_type in ("boolean", "labeled_boolean"):
                kwargs[_key] = "logical_or"
            elif data_type == "event" and source_type == "event_stream":
                kwargs[_key] = "countif"
            else:
                kwargs[_key] = "sum"

        if data_type.startswith("labeled_"):
            if (_key := "label_aggregator") not in kwargs:
                kwargs[_key] = kwargs["ping_aggregator"]
            return LabeledMetric(**kwargs)
        if data_type == "event":
            return EventMetric(**kwargs)
        return Metric(**kwargs)


@dataclass
class LabeledMetric(Metric):
    label_aggregator: str
    label_in: None | list[str] = None


@dataclass
class EventMetric(Metric):
    category: str
    event_name: str


@dataclass
class Feature:
    name: str
    project: str
    dataset: str
    metrics_by_source: dict[str, list[Metric]]
    ratios: list[list[str, str]]

    def all_metrics(self):
        return [
            (metric, aggregator)
            for metrics in self.metrics_by_source.values()
            for metric in metrics
            for aggregator in metric.aggregators
        ]


def generate_queries(project, path, write_dir, config_path=None):
    """Generate nimbus feature monitoring queries."""
    if config_path is None:
        config_path = path
    env = Environment(
        loader=FileSystemLoader(template_dir := path / "nimbus_feature_monitoring_v1"),
        keep_trailing_newline=True,
    )
    sql_template = env.get_template("query.sql")
    metadata_template = env.get_template("metadata.yaml")
    view_template = env.get_template("view.sql")
    schema = (template_dir / "schema.yaml").read_text()
    for app_config_path in config_path.glob("*.toml"):
        app_config = FeatmonSpec.from_file(app_config_path)
        dataset = app_config.dataset
        source_tables = {}
        for source_name, source in app_config.data_sources.items():
            dimensions = []
            for dim_name, dim in source.dimensions.items():
                if dim is None:
                    dim = {}
                else:
                    dim = dict(dim)
                dimensions.append(
                    Dimension(
                        name=dim_name,
                        field=dim.pop("field", dim_name),
                        **dim,
                    )
                )
            source_tables[source_name] = SourceTable(
                name=source_name,
                project=project,
                dataset=dataset,
                dimensions=dimensions,
                table_name=source.table_name,
                analysis_unit_id=source.analysis_unit_id,
                type=source.type,
            )
        features = []
        for feat_name, feat in app_config.features.items():
            metrics_by_source = {}
            for source_name, source_metrics in feat.metrics_by_source.items():
                metrics_by_source[source_name] = metrics = []
                for data_type, data_type_metrics in source_metrics.items():
                    if data_type == "event":
                        for category, category_metrics in data_type_metrics.items():
                            for metric_name, metric in category_metrics.items():
                                if metric is None:
                                    metric = {}
                                metrics.append(
                                    Metric.from_(
                                        name=f"{category}_{metric_name}",
                                        data_type=data_type,
                                        category=category,
                                        event_name=metric.pop(
                                            "event_name", metric_name
                                        ),
                                        source_type=source_tables[
                                            source_name
                                        ].type,
                                        **metric,
                                    )
                                )
                    else:
                        for metric_name, metric in data_type_metrics.items():
                            if metric is None:
                                metric = {}
                            metrics.append(
                                Metric.from_(
                                    name=metric_name, data_type=data_type, **metric
                                )
                            )
            features.append(
                Feature(
                    name=feat_name,
                    project=project,
                    dataset=f"{dataset}_derived",
                    ratios=[],
                    metrics_by_source=metrics_by_source,
                )
            )

        feature_tables = []
        for feature in features:
            args = {
                "source_tables": [
                    t
                    for t in source_tables.values()
                    if t.dimensions or (t.name in feature.metrics_by_source)
                ],
                "feature": feature,
            }

            feature_name_sql = feature.name.replace("-", "_").lower()
            table = f"nimbus_feature_monitoring_{feature_name_sql}_v1"
            sql_table_name = f"{feature.project}.{feature.dataset}.{table}"

            write_sql(
                write_dir / project,
                sql_table_name,
                "query.sql",
                reformat(sql_template.render(**args)),
            )
            write_path = Path(write_dir) / feature.project / feature.dataset / table
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
    help="Where query directories (SQL templates) will be searched for.",
    default="sql_generators/nimbus_feature_monitoring/templates",
    required=False,
    type=click.Path(file_okay=False),
)
@click.option(
    "--config-path",
    "--config_path",
    help=(
        "Directory containing TOML app config files (e.g. from metric-hub). "
        "Defaults to --path if not specified."
    ),
    default=None,
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
def generate(target_project, path, config_path, output_dir, use_cloud_function):
    """Generate the nimbus feature monitoring views."""
    generate_queries(
        target_project,
        Path(path),
        Path(output_dir),
        config_path=Path(config_path) if config_path else None,
    )
