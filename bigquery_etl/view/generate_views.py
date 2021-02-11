"""
DEPRECATED in favor of generate_stable_views.

This module will be removed once ops logic transitions to use the new module.

Generates one view definition file per document type in '_stable' tables.

If there are existing view definitions in the destination directory then those will be
kept instead.

Run as:
  ./script/generate_views 'moz-fx-data-shared-prod:*_stable.*'
"""

import logging
import os
import re
from argparse import ArgumentParser
from fnmatch import fnmatchcase
from functools import partial
from multiprocessing.pool import ThreadPool

from google.cloud import bigquery

from bigquery_etl.util import standard_args
from bigquery_etl.util.bigquery_tables import get_tables_matching_patterns

VERSION_RE = re.compile(r"_v([0-9]+)$")
WHITESPACE_RE = re.compile(r"\s+")
WILDCARD_RE = re.compile(r"[*?[]")

DEFAULT_PATTERN = "telemetry.*"
DEFAULT_EXCLUDE = r"*_raw"

VIEW_QUERY_TEMPLATE = """
SELECT
  * REPLACE(
    {replacements})
FROM
  `{target}`
"""

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "patterns",
    metavar="[project:]dataset[.table]",
    default=[DEFAULT_PATTERN],
    nargs="*",
    help="Table that should have a latest-version view, may use shell-style wildcards,"
    f" defaults to: {DEFAULT_PATTERN}",
)
parser.add_argument(
    "--exclude",
    action="append",
    default=[DEFAULT_EXCLUDE],
    metavar="project:dataset.table",
    help="Latest-version views that should be ignored, may use shell-style wildcards,"
    f" defaults to: {DEFAULT_EXCLUDE}",
)
parser.add_argument(
    "--sql-dir", default="sql/", help="The path where generated SQL files are stored."
)
standard_args.add_log_level(parser)
standard_args.add_parallelism(parser)


def main():
    """Generate view definitions."""
    args = parser.parse_args()

    # set log level
    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    client = bigquery.Client()
    tables = get_tables_matching_patterns(client, args.patterns)

    views = {}
    for full_table_id in tables:
        view = VERSION_RE.sub("", full_table_id)
        if view not in views:
            views[view] = []
        views[view].append(full_table_id)

    with ThreadPool(args.parallelism) as pool:
        pool.starmap(
            partial(
                create_view_if_not_exists,
                client,
                exclude=args.exclude,
                sql_dir=args.sql_dir,
            ),
            views.items(),
        )


def create_view_if_not_exists(client, view, tables, exclude, sql_dir):
    """Create view unless a local file for creating the view exists."""
    if any(fnmatchcase(pattern, view) for pattern in exclude):
        logging.info("skipping table: matched by exclude pattern: {view}")
        return
    if view.endswith("_"):
        # A trailing '_' confuses the logic here of parsing versions,
        # and likely indicates that the table is somehow private, so
        # we ignore it.
        logging.info("skipping table ending in _: {view}")
        return

    version = max(
        int(match.group()[2:])
        for table in tables
        for match in [VERSION_RE.search(table)]
        if match is not None
    )

    project, dataset, viewname = view.split(".")
    target = f"{view}_v{version}"
    view_dataset = dataset.rsplit("_", 1)[0]
    full_view_id = ".".join([project, view_dataset, viewname])
    target_file = os.path.join(sql_dir, project, view_dataset, viewname, "view.sql")

    if not os.path.exists(target_file):
        # We put this BQ API all inside the conditional to speed up execution
        # in the case target files already exist.
        table = client.get_table(target)
        replacements = ["mozfun.norm.metadata(metadata)" " AS metadata"]
        # Add special replacements for Glean pings.
        schema_id = table.labels.get("schema_id", None)
        if schema_id == "glean_ping_1":
            replacements += ["mozfun.norm.glean_ping_info(ping_info)" " AS ping_info"]
            if table.table_id == "baseline_v1":
                replacements += [
                    "mozfun.norm.glean_baseline_client_info"
                    "(client_info, metrics)"
                    " AS client_info"
                ]
            if table.dataset_id.startswith(
                "org_mozilla_fenix"
            ) and table.table_id.startswith("metrics"):
                # todo: use mozfun udfs
                replacements += [
                    "`moz-fx-data-shared-prod.udf.normalize_fenix_metrics`"
                    "(client_info.telemetry_sdk_build, metrics)"
                    " AS metrics"
                ]
            if table.dataset_id.startswith("firefox_desktop"):
                # Bug 1672191
                replacements += [
                    "'Firefox' AS normalized_app_name",
                ]
        elif schema_id in ("main_ping_1", "main_ping_4"):
            replacements += [
                "`moz-fx-data-shared-prod.udf.normalize_main_payload`(payload)"
                " AS payload"
            ]
        replacements = ",\n    ".join(replacements)
        view_query = VIEW_QUERY_TEMPLATE.format(
            target=target, replacements=replacements
        ).strip()
        full_sql = f"CREATE OR REPLACE VIEW\n  `{full_view_id}`\nAS {view_query}\n"
        print("Creating " + target_file)
        if not os.path.exists(os.path.dirname(target_file)):
            os.makedirs(os.path.dirname(target_file))
        with open(target_file, "w") as f:
            f.write(full_sql)


if __name__ == "__main__":
    main()
