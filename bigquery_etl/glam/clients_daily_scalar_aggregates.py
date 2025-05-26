#!/usr/bin/env python3
"""clients_daily_scalar_aggregates query generator."""
import argparse
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.probe_filters import get_etl_excluded_probes_quickfix

from .client_side_sampled_metrics import get as get_sampled_metrics
from .utils import get_schema, ping_type_from_table

ATTRIBUTES = ",".join(
    [
        "client_id",
        "ping_type",
        "submission_date",
        "os",
        "app_version",
        "app_build_id",
        "channel",
    ]
)


def render_main(**kwargs):
    """Create a SQL query for the clients_daily_scalar_aggregates dataset."""
    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))
    main_sql = env.get_template("clients_daily_scalar_aggregates_v1.sql")
    return reformat(main_sql.render(**kwargs))


def get_labeled_metrics_sql(probes: Dict[str, List[str]]) -> str:
    """Get the SQL for labeled scalar metrics."""
    probes_struct = []
    for metric_type, _probes in probes.items():
        for probe in _probes:
            probes_struct.append(
                f"('{probe}', '{metric_type}', metrics.{metric_type}.{probe})"
            )

    probes_struct.sort()
    probes_arr = ",\n".join(probes_struct)
    return probes_arr


def get_unlabeled_metrics_sql(probes: Dict[str, List[str]]) -> str:
    """Put together the subsets of SQL required to query scalars or booleans."""
    probe_structs = []
    for probe in probes.pop("boolean", []):
        probe_structs.append(
            (
                f"('{probe}', 'boolean', '', 'false', "
                f"SUM(CAST(NOT metrics.boolean.{probe} AS INT64)))"
            )
        )
        probe_structs.append(
            (
                f"('{probe}', 'boolean', '', 'true', "
                f"SUM(CAST(metrics.boolean.{probe} AS INT64)))"
            )
        )

    for metric_type, _probes in probes.items():
        # timespans are nested within an object that also carries the unit of
        # of time associated with the value
        suffix = ".value" if metric_type == "timespan" else ""
        for probe in _probes:
            for agg_func in ["max", "avg", "min", "sum"]:
                probe_structs.append(
                    (
                        f"('{probe}', '{metric_type}', '', '{agg_func}', "
                        f"{agg_func}(CAST(metrics.{metric_type}.{probe}{suffix} AS NUMERIC)))"
                    )
                )
            probe_structs.append(
                f"('{probe}', '{metric_type}', '', 'count', "
                f"IF(MIN(metrics.{metric_type}.{probe}{suffix}) IS NULL, NULL, COUNT(*)))"
            )

    probe_structs.sort()
    probes_arr = ",\n".join(probe_structs)
    return probes_arr


def get_scalar_metrics(
    schema: Dict, scalar_type: str
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """Find all scalar probes in a Glean table.

    Metric types are defined in the Glean documentation found here:
    https://mozilla.github.io/glean/book/user/metrics/index.html
    """
    assert scalar_type in ("unlabeled", "labeled")
    metric_type_set = {
        "unlabeled": ["boolean", "counter", "quantity", "timespan"],
        "labeled": ["labeled_counter"],
    }
    scalars: Dict[str, List[str]] = {
        metric_type: [] for metric_type in metric_type_set[scalar_type]
    }
    excluded_metrics = get_etl_excluded_probes_quickfix("fenix")

    # Metrics that are already sampled
    sampled_metric_names = get_sampled_metrics("counters")
    sampled_metrics = {"counter": sampled_metric_names}
    found_sampled_metrics = defaultdict(list)

    # Iterate over every element in the schema under the metrics section and
    # collect a list of metric names.
    for root_field in schema:
        if root_field["name"] != "metrics":
            continue
        for metric_field in root_field["fields"]:
            metric_type = metric_field["name"]
            if metric_type not in metric_type_set[scalar_type]:
                continue
            for field in metric_field["fields"]:
                if field["name"] in sampled_metrics.get(metric_type, []):
                    found_sampled_metrics[metric_type].append(field["name"])
                elif field["name"] not in excluded_metrics:
                    scalars[metric_type].append(field["name"])

    return scalars, found_sampled_metrics


def main():
    """Print a clients_daily_scalar_aggregates query to stdout."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-parameterize",
        action="store_true",
        help="Generate a query without parameters",
    )
    parser.add_argument("--source-table", type=str, help="Name of Glean table")
    parser.add_argument(
        "--product",
        type=str,
        default="org_mozilla_fenix",
    )
    args = parser.parse_args()

    # If set to 1 day, then runs of copy_deduplicate may not be done yet
    submission_date = (
        "date_sub(current_date, interval 2 day)"
        if args.no_parameterize
        else "@submission_date"
    )
    header = (
        "-- Query generated by: python3 -m "
        "bigquery_etl.glam.clients_daily_scalar_aggregates "
        f"--source-table {args.source_table}"
        + (" --no-parameterize" if args.no_parameterize else "")
    )

    schema = get_schema(args.source_table)
    unlabeled_metric_names, unlabeled_sampled_metric_names = get_scalar_metrics(
        schema, "unlabeled"
    )
    labeled_metric_names, _ = get_scalar_metrics(schema, "labeled")
    unlabeled_metrics = get_unlabeled_metrics_sql(unlabeled_metric_names).strip()
    labeled_metrics = get_labeled_metrics_sql(labeled_metric_names).strip()
    client_sampled_metrics_sql = {"labeled": [], "unlabeled": []}
    if args.product == "firefox_desktop":
        client_sampled_metrics_sql["unlabeled"] = get_unlabeled_metrics_sql(
            unlabeled_sampled_metric_names
        ).strip()
    if not unlabeled_metrics and not labeled_metrics:
        print(header)
        print("-- Empty query: no probes found!")
        sys.exit(1)
    print(
        render_main(
            header=header,
            source_table=args.source_table,
            submission_date=submission_date,
            attributes=ATTRIBUTES,
            unlabeled_metrics=unlabeled_metrics,
            labeled_metrics=labeled_metrics,
            ping_type=ping_type_from_table(args.source_table),
            client_sampled_unlabeled_metrics=client_sampled_metrics_sql["unlabeled"],
            client_sampled_labeled_metrics=client_sampled_metrics_sql["labeled"],
            client_sampled_channel="release",
            client_sampled_os="Windows",
            client_sampled_max_sample_id=100,
        )
    )


if __name__ == "__main__":
    main()
