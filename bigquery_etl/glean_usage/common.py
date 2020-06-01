"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
from pathlib import Path

from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.bigquery_id import sql_table_id  # noqa E402


def render(sql_filename, **kwargs) -> str:
    """Render a given template query using Jinja."""
    env = Environment(loader=PackageLoader("bigquery_etl", "glean_usage/templates"))
    main_sql = env.get_template(sql_filename)
    return reformat(main_sql.render(**kwargs))


def write_sql(output_dir, full_table_id, basename, sql):
    """Write out a query to a location based on the table ID.

    :param output_dir:    Base target directory (probably sql/)
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


def list_baseline_tables(client, pool, project_id, only_tables, table_filter):
    """Make parallel BQ API calls to grab all Glean baseline stable tables.

    See `util.standard_args` for more context on table filtering.

    :param client:       A BigQuery client object
    :param pool:         A process pool for handling concurrent calls
    :param project_id:   Target project
    :param only_tables:  An iterable of globs in `<stable_dataset>.<table>` format
    :param table_filter: A function for determining whether to process a table
    :return:             A list of matching tables
    """
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [f"{project_id}.{t}" for t in only_tables if table_filter(t)]
    if only_tables and not _contains_glob(
        _extract_dataset_from_glob(t) for t in only_tables
    ):
        # skip list_datasets call when only_tables exists and datasets contain no globs
        stable_datasets = {
            f"{project_id}.{_extract_dataset_from_glob(t)}" for t in only_tables
        }
    else:
        stable_datasets = [
            d.reference.dataset_id for d in client.list_datasets(project_id)
        ]
    stable_datasets = [d for d in stable_datasets if d.endswith("_stable")]
    return [
        sql_table_id(t)
        for tables in pool.map(client.list_tables, stable_datasets)
        for t in tables
        if table_filter(f"{t.dataset_id}.{t.table_id}")
        and t.table_id == "baseline_v1"
        and t.labels.get("schema_id") == "glean_ping_1"
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


def _contains_glob(patterns):
    return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


def _extract_dataset_from_glob(pattern):
    # Assumes globs are in <dataset>.<table> form without a project specified.
    return pattern.split(".", 1)[0]
