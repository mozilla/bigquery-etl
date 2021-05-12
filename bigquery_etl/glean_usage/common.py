"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
from jinja2 import TemplateNotFound
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


def table_names_from_baseline(baseline_table, include_project_id=True):
    """Return a dict with full table IDs for derived tables and views.

    :param baseline_table: stable table ID in project.dataset.table form
    """
    prefix = re.sub(r"_stable\..+", "", baseline_table)
    if not include_project_id:
        prefix = ".".join(prefix.split(".")[1:])
    return dict(
        baseline_table=f"{prefix}_stable.baseline_v1",
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


def generate(
    project_id,
    baseline_table,
    prefix,
    target_table_id,
    custom_render_kwargs={},
    no_init=True,
    output_dir=None,
    output_only=False,
):
    """Generate the baseline table query."""
    tables = table_names_from_baseline(baseline_table, include_project_id=False)

    init_filename = f"{target_table_id}.init.sql"
    query_filename = f"{target_table_id}.query.sql"
    view_filename = f"{target_table_id[:-3]}.view.sql"
    view_metadata_filename = f"{target_table_id[:-3]}.metadata.yaml"

    table = tables[f"{prefix}_table"]
    view = tables[f"{prefix}_view"]
    render_kwargs = dict(
        header="-- Generated via bigquery_etl.glean_usage\n",
        project_id=project_id,
    )
    render_kwargs.update(custom_render_kwargs)
    render_kwargs.update(tables)

    query_sql = render(query_filename, **render_kwargs)
    view_sql = render(view_filename, **render_kwargs)
    view_metadata = render(view_metadata_filename, format=False, **render_kwargs)

    if not no_init:
        try:
            init_sql = render(init_filename, **render_kwargs)
        except TemplateNotFound:
            init_sql = render(query_filename, init=True, **render_kwargs)

    if not (referenced_table_exists(view_sql)):
        logging.info("Skipping view for table which doesn't exist:" f" {target_table_id}")
        return

    if output_dir:
        write_sql(output_dir, view, "metadata.yaml", view_metadata)
        write_sql(output_dir, view, "view.sql", view_sql)
        write_sql(output_dir, table, "query.sql", query_sql)

        if not no_init:
            write_sql(output_dir, table, "init.sql", init_sql)

    if output_only:
        return

    # dry run generated query
    DryRun(
        os.path.join(output_dir, *list(baseline_table.split(".")[-2:]), query_filename),
        query_sql,
    ).is_valid()
