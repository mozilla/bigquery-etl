#!/usr/bin/env python3
"""
Generates the experiment monitoring search metrics materialized views.

Materialized views currently do not support UNNESTing of fields in
sub-queries. To ensure the experiment search metrics numbers are
correcy, a materialized view has to be created for each dataset and
each search metric. The materialized views are then combined in a view
to make querying more convenient.
"""
from argparse import ArgumentParser
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

import os
import sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from bigquery_etl.metadata.parse_metadata import Metadata, METADATA_FILE  # noqa E402


FILE_PATH = os.path.dirname(__file__)
BASE_DIR = Path(FILE_PATH).parent.parent
TARGET_DIR = BASE_DIR / "sql" / "moz-fx-data-shared-prod"
MATERIALIZED_VIEW_TEMPLATE_PATH = Path(FILE_PATH) / "templates"
MATERIALIZED_VIEW_TEMPLATE = "materialized_view.sql"
SEARCH_METRICS_VIEW_TEMPLATE = "search_metrics_view.sql"
SEARCH_METRICS = {
    "fenix": {
        "base_tables": [
            "moz-fx-data-shared-prod.org_mozilla_fenix_live.metrics_v1",
            "moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.metrics_v1",
            "moz-fx-data-shared-prod.org_mozilla_firefox_live.metrics_v1",
        ],
        "metrics": [
            {"ad_clicks_count": "metrics.labeled_counter.browser_search_ad_clicks"},
            {
                "search_with_ads_count": "metrics.labeled_counter.browser_search_with_ads"
            },
            {"search_count": "metrics.labeled_counter.metrics_search_count"},
        ],
        "experiment_field": "ping_info.experiments",
    },
    "desktop": {
        "base_tables": ["moz-fx-data-shared-prod.telemetry_live.main_v4"],
        "metrics": [
            {
                "ad_clicks_count": "payload.processes.parent.keyed_scalars.browser_search_ad_clicks"  # noqa E501
            },
            {
                "search_with_ads_count": "payload.processes.parent.keyed_scalars.browser_search_with_ads"  # noqa E501
            },
            {"search_count": "payload.keyed_histograms.search_counts"},
        ],
        "experiment_field": "environment.experiments",
    },
}


def generate_materialized_views(owner):
    """Generate the materialized experiment search metrics views."""
    for platform, search_metrics in SEARCH_METRICS.items():
        for base_table in search_metrics["base_tables"]:
            for metric in search_metrics["metrics"]:
                # generate materialized view from template
                env = Environment(
                    loader=FileSystemLoader(str(MATERIALIZED_VIEW_TEMPLATE_PATH))
                )
                template = env.get_template(MATERIALIZED_VIEW_TEMPLATE)
                derived_dataset = base_table.split(".")[1].replace("_live", "_derived")
                metric_name = list(metric.keys())[0]

                output = template.render(
                    platform=platform,
                    metric=metric_name,
                    probe=list(metric.values())[0],
                    dataset=derived_dataset,
                    base_table=base_table,
                    experiment=search_metrics["experiment_field"],
                )

                # create target directory
                output_dir = (
                    TARGET_DIR / derived_dataset / f"experiment_{metric_name}_live_v1"
                )
                output_dir.mkdir(parents=True, exist_ok=True)

                # write materialized view to SQL file
                output_file = output_dir / "init.sql"
                output_file.write_text(output)

                print(f"Generate {output_file}")

                # Generate metadata file
                metadata = Metadata(
                    friendly_name=f"Experiment {metric_name.replace('_', ' ')} live",
                    description=(
                        f"Materialized view of aggregated number of {metric_name} "
                        "of clients enrolled in experiments."
                    ),
                    owners=[owner],
                    labels={"materialized_view": True},
                )
                metadata.write(output_dir / METADATA_FILE)


def generate_search_metrics_view(owner):
    """Generate the view that combines experiment search metrics from all datasets."""
    datasets = [
        dataset.split(".")[1].replace("_live", "_derived")
        for _, platform in SEARCH_METRICS.items()
        for dataset in platform["base_tables"]
    ]

    metrics = list(
        set(
            [
                list(metric.keys())[0]
                for _, platform in SEARCH_METRICS.items()
                for metric in platform["metrics"]
            ]
        )
    )

    env = Environment(loader=FileSystemLoader(str(MATERIALIZED_VIEW_TEMPLATE_PATH)))
    template = env.get_template(SEARCH_METRICS_VIEW_TEMPLATE)
    output = template.render(datasets=datasets, metrics=metrics)

    # create target directory
    output_dir = (
        TARGET_DIR / "telemetry_derived" / "experiment_search_aggregates_live_v1"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # write materialized view to SQL file
    output_file = output_dir / "view.sql"
    output_file.write_text(output)

    print(f"Generate {output_file}")

    # Generate metadata file
    metadata = Metadata(
        friendly_name="Experiment Search Aggregates Live",
        description="View for live experiment search metric aggregates.",
        owners=[owner],
        labels={"incremental": False},
    )
    metadata.write(output_dir / METADATA_FILE)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--owner", help="Email address of owner for materialized views")
    args = parser.parse_args()
    generate_materialized_views(args.owner)
    generate_search_metrics_view(args.owner)
