"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
import tempfile
from functools import partial
from pathlib import Path

import requests
from jinja2 import Environment, PackageLoader

from bigquery_etl.dryrun import DryRun
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.bigquery_id import sql_table_id  # noqa E402


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


def baseline_exists_in_dataset(project_id, dataset_id):
    """Dry run a simple query that tests if a baseline table exists.

    It's possible that the generated-schemas branch contains new doctypes
    that have yet gone through ops logic to create the associated BQ
    resources, and this helper lets us skip those.
    """
    with tempfile.TemporaryDirectory() as tdir:
        tfile = Path(tdir) / "query.sql"
        with tfile.open("w") as f:
            f.write(
                f"SELECT * FROM `{project_id}.{dataset_id}.baseline_v1`"
                " WHERE DATE(submission_timestamp) = '2000-01-01'"
            )
            dryrun = DryRun(str(tfile))
            exists = 404 not in [e.get("code") for e in dryrun.errors()]
            return (dataset_id, exists)


def list_baseline_tables(pool, project_id, only_tables, table_filter):
    """Return Glean app listings from the probeinfo API."""
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [f"{project_id}.{t}" for t in only_tables if table_filter(t)]
    if only_tables and not _contains_glob(
        _extract_dataset_from_glob(t) for t in only_tables
    ):
        stable_datasets = {_extract_dataset_from_glob(t) for t in only_tables}
        stable_datasets = {d for d in stable_datasets if d.endswith("_stable")}
    else:
        response = requests.get(
            "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
        )
        apps = response.json()
        stable_datasets = [
            f"{app['bq_dataset_family']}_stable"
            for app in apps
            # TODO: consider filtering out deprecated app listings.
            # if not app.get("deprecated", False)
        ]
    return [
        f"{project_id}.{d}.baseline_v1"
        for d, exists in pool.map(
            partial(baseline_exists_in_dataset, project_id), stable_datasets
        )
        if exists and table_filter(f"{d}.baseline_v1")
    ]


def table_names_from_baseline(baseline_table):
    """Return a dict with full table IDs for derived tables and views.

    :param baseline_table: stable table ID in project.dataset.table form
    """
    prefix = re.sub(r"_stable\..+", "", baseline_table)
    return dict(
        baseline_table=baseline_table,
        daily_table=f"{prefix}_derived.baseline_clients_daily_v1",
        last_seen_table=f"{prefix}_derived.baseline_clients_last_seen_v1",
        daily_view=f"{prefix}.baseline_clients_daily",
        last_seen_view=f"{prefix}.baseline_clients_last_seen",
    )


def referenced_table_exists(view_sql):
    """Dry run the given view SQL to see if its referent exists."""
    with tempfile.TemporaryDirectory() as tdir:
        tfile = Path(tdir) / "view.sql"
        with tfile.open("w") as f:
            f.write(view_sql)
        dryrun = DryRun(str(tfile))
        return 404 not in [e.get("code") for e in dryrun.errors()]


def _contains_glob(patterns):
    return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


def _extract_dataset_from_glob(pattern):
    # Assumes globs are in <dataset>.<table> form without a project specified.
    return pattern.split(".", 1)[0]
