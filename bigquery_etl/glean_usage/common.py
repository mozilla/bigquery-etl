"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
import requests
from jinja2 import TemplateNotFound
from pathlib import Path

from jinja2 import Environment, PackageLoader

from bigquery_etl.dryrun import DryRun
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util import standard_args  # noqa E402
from bigquery_etl.util.bigquery_id import sql_table_id  # noqa E402
from bigquery_etl.view import generate_stable_views

APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"


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


def write_dataset_metadata(output_dir, full_table_id):
    """
    Add dataset_metadata.yaml to public facing datasets.

    Does not overwrite existing dataset_metadata.yaml files.
    """
    d = Path(os.path.join(output_dir, *list(full_table_id.split(".")[-2:])))
    target = d.parent / "dataset_metadata.yaml"

    public_facing = all(
        [postfix not in d.parent.name for postfix in ("_derived", "_stable")]
    )
    if public_facing and not target.exists():
        env = Environment(loader=PackageLoader("bigquery_etl", "glean_usage/templates"))
        dataset_metadata = env.get_template("dataset_metadata.yaml")
        rendered = dataset_metadata.render(
            {
                "friendly_name": " ".join(
                    [p.capitalize() for p in d.parent.name.split("_")]
                ),
                "dataset": d.parent.name,
            }
        )

        logging.info(f"Writing {target}")
        target.write_text(rendered)


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


def get_app_info():
    """Return a list of applications from the probeinfo API."""
    resp = requests.get(APP_LISTINGS_URL)
    resp.raise_for_status()
    apps_json = resp.json()
    app_info = {}

    for app in apps_json:
        if app["app_name"] not in app_info:
            app_info[app["app_name"]] = [app]
        else:
            app_info[app["app_name"]].append(app)

    return app_info


class GleanTable:
    """Represents a generated Glean table."""

    def __init__(self):
        """Init Glean table."""
        self.target_table_id = ""
        self.prefix = ""
        self.custom_render_kwargs = {}
        self.no_init = True

    def generate_per_app_id(self, project_id, baseline_table, output_dir=None):
        """Generate the baseline table query per app_id."""
        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.init.sql"
        query_filename = f"{self.target_table_id}.query.sql"
        view_filename = f"{self.target_table_id[:-3]}.view.sql"
        view_metadata_filename = f"{self.target_table_id[:-3]}.metadata.yaml"

        table = tables[f"{self.prefix}_table"]
        view = tables[f"{self.prefix}_view"]
        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        query_sql = render(query_filename, **render_kwargs)
        view_sql = render(view_filename, **render_kwargs)
        view_metadata = render(view_metadata_filename, format=False, **render_kwargs)

        if not self.no_init:
            try:
                init_sql = render(init_filename, **render_kwargs)
            except TemplateNotFound:
                init_sql = render(query_filename, init=True, **render_kwargs)

        if not (referenced_table_exists(view_sql)):
            logging.info(
                "Skipping view for table which doesn't exist:"
                f" {self.target_table_id}"
            )
            return

        if output_dir:
            write_sql(output_dir, view, "metadata.yaml", view_metadata)
            write_sql(output_dir, view, "view.sql", view_sql)
            write_sql(output_dir, table, "query.sql", query_sql)

            if not self.no_init:
                write_sql(output_dir, table, "init.sql", init_sql)

            write_dataset_metadata(output_dir, view)

    def generate_per_app(self, project_id, app_info, output_dir=None):

        target_view_name = "_".join(self.target_table_id.split("_")[:-1])
        target_dataset = app_info[0]["app_name"]

        datasets = [
            (a["bq_dataset_family"], a["app_channel"])
            for a in app_info
            if "app_channel" in a
        ]

        # Some apps only have a single channel, in which case the per-app_id dataset
        # is already sufficient.
        if len(datasets) == 0:
            return

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{target_dataset}.{target_view_name}",
            datasets=datasets,
            table=target_view_name,
        )

        sql = render("cross_channel.view.sql", **render_kwargs)

        if not (referenced_table_exists(sql)):
            logging.info(
                "Skipping view for table which doesn't exist:"
                f" {self.target_table_id}"
            )
            return

        view = f"{project_id}.{target_dataset}.{target_view_name}"

        if output_dir:
            write_sql(output_dir, view, "view.sql", sql)
            write_dataset_metadata(output_dir, view)
