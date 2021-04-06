"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
from argparse import ArgumentParser
from datetime import datetime
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path

from jinja2 import Environment, PackageLoader

from bigquery_etl.dryrun import DryRun
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util import standard_args  # noqa E402
from bigquery_etl.util.bigquery_id import sql_table_id  # noqa E402
from bigquery_etl.view import generate_stable_views


def render(sql_filename, format=True, **kwargs) -> str:
    """Render a given template query using Jinja."""
    env = Environment(loader=PackageLoader("bigquery_etl", "glean_usage/templates"))
    main_sql = env.get_template(sql_filename)
    rendered = main_sql.render(**kwargs)
    if format:
        rendered = reformat(rendered)
    return rendered


def write_sql(output_dir, full_table_id, basename, sql):
    """Write out a query to a location based on the table ID.

    :param output_dir:    Base target directory (probably sql/moz-fx-data-shared-prod/)
    :param full_table_id: Table ID in project.dataset.table form
    :param basename:      The name to give the written file (like query.sql)
    :param sql:           The query content to write out
    """
    d = Path(os.path.join(output_dir, *list(full_table_id.split(".")[-2:])))
    d.mkdir(parents=True, exist_ok=True)
    target = d / basename
    logging.info(f"Writing {target}")
    with target.open("w") as f:
        f.write(sql)
        f.write("\n")


def list_baseline_tables(project_id, only_tables, table_filter):
    """Return names of all matching baseline tables in shared-prod."""
    prod_baseline_tables = [
        s.stable_table
        for s in generate_stable_views.get_stable_table_schemas()
        if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
        and s.bq_table == "baseline_v1"
    ]
    prod_datasets_with_baseline = [t.split(".")[0] for t in prod_baseline_tables]
    stable_datasets = prod_datasets_with_baseline
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [
            f"{project_id}.{t}"
            for t in only_tables
            if table_filter(t) and t in prod_baseline_tables
        ]
    if only_tables and not _contains_glob(
        _extract_dataset_from_glob(t) for t in only_tables
    ):
        stable_datasets = {_extract_dataset_from_glob(t) for t in only_tables}
        stable_datasets = {
            d
            for d in stable_datasets
            if d.endswith("_stable") and d in prod_datasets_with_baseline
        }
    return [
        f"{project_id}.{d}.baseline_v1"
        for d in stable_datasets
        if table_filter(f"{d}.baseline_v1")
    ]


def table_names_from_baseline(baseline_table):
    """Return a dict with full table IDs for derived tables and views.

    :param baseline_table: stable table ID in project.dataset.table form
    """
    prefix = re.sub(r"_stable\..+", "", baseline_table)
    return dict(
        baseline_table=baseline_table,
        migration_table=f"{prefix}_stable.migration_v1",
        daily_table=f"{prefix}_derived.baseline_clients_daily_v1",
        last_seen_table=f"{prefix}_derived.baseline_clients_last_seen_v1",
        first_seen_table=f"{prefix}_derived.baseline_clients_first_seen_v1",
        daily_view=f"{prefix}.baseline_clients_daily",
        last_seen_view=f"{prefix}.baseline_clients_last_seen",
        first_seen_view=f"{prefix}.baseline_clients_first_seen",
    )


def referenced_table_exists(view_sql):
    """Dry run the given view SQL to see if its referent exists."""
    dryrun = DryRun("foo/bar/view.sql", content=view_sql)
    return 404 not in [e.get("code") for e in dryrun.errors()]


def _contains_glob(patterns):
    return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


def _extract_dataset_from_glob(pattern):
    # Assumes globs are in <dataset>.<table> form without a project specified.
    return pattern.split(".", 1)[0]


def get_argument_parser(description):
    """Get the argument parser for the shared glean usage queries."""
    parser = ArgumentParser(description=description)
    parser.add_argument(
        "--project_id",
        "--project-id",
        default="moz-fx-data-shar-nonprod-efed",
        help="ID of the project in which to find tables",
    )
    parser.add_argument(
        "--date",
        required=True,
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
        help="Date partition to process, in format 2019-01-01",
    )
    parser.add_argument(
        "--output_dir",
        "--output-dir",
        help="Also write the query text underneath the given sql dir",
    )
    parser.add_argument(
        "--output_only",
        "--output-only",
        "--views_only",  # Deprecated name
        "--views-only",  # Deprecated name
        action="store_true",
        help=(
            "If set, we only write out sql to --output-dir and we skip"
            " running the queries"
        ),
    )
    standard_args.add_parallelism(parser)
    standard_args.add_dry_run(parser, debug_log_queries=False)
    standard_args.add_log_level(parser)
    standard_args.add_priority(parser)
    standard_args.add_billing_projects(parser)
    standard_args.add_table_filter(parser)
    return parser


def generate_and_run_query(run_query_callback, description):
    """Generates and runs queries using a threadpool.

    The callback function is reponsible for generating and running the queries.
    This was the original main entrypoint for each of the usage queries."""

    parser = get_argument_parser(description)
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    baseline_tables = list_baseline_tables(
        project_id=args.project_id,
        only_tables=getattr(args, "only_tables", None),
        table_filter=args.table_filter,
    )

    with ThreadPool(args.parallelism) as pool:
        # Do a first pass with dry_run=True so we don't end up with a partial success;
        # we also write out queries in this pass if so configured.
        pool.map(
            partial(
                run_query_callback,
                args.project_id,
                date=args.date,
                dry_run=True,
                output_dir=args.output_dir,
                output_only=args.output_only,
            ),
            baseline_tables,
        )
        if args.output_only:
            return
        logging.info(f"Dry runs successful for {len(baseline_tables)} table(s)")
        # Now, actually run the queries.
        if not args.dry_run:
            pool.map(
                partial(
                    run_query_callback, args.project_id, date=args.date, dry_run=False
                ),
                baseline_tables,
            )
