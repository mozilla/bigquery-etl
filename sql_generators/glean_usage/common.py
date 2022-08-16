"""Utility functions used in generating usage queries on top of Glean."""

import logging
import os
import re
from pathlib import Path

import requests
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from bigquery_etl.dryrun import DryRun
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from bigquery_etl.util.common import render, write_sql

APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
PATH = Path(os.path.dirname(__file__))


def write_dataset_metadata(output_dir, full_table_id, derived_dataset_metadata=False):
    """
    Add dataset_metadata.yaml to public facing datasets.

    Does not overwrite existing dataset_metadata.yaml files.
    """
    d = Path(os.path.join(output_dir, *list(full_table_id.split(".")[-2:])))
    d.parent.mkdir(parents=True, exist_ok=True)
    target = d.parent / "dataset_metadata.yaml"

    public_facing = all(
        [postfix not in d.parent.name for postfix in ("_derived", "_stable")]
    )
    if (derived_dataset_metadata or public_facing) and not target.exists():
        env = Environment(loader=FileSystemLoader(PATH / "templates"))
        if derived_dataset_metadata:
            dataset_metadata = env.get_template("derived_dataset_metadata.yaml")
        else:
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
        for s in get_stable_table_schemas()
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
        if app["app_id"].startswith("rally"):
            pass
        elif app["app_name"] not in app_info:
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
        self.per_app_id_enabled = True
        self.per_app_enabled = True
        self.cross_channel_template = "cross_channel.view.sql"

    def generate_per_app_id(self, project_id, baseline_table, output_dir=None):
        """Generate the baseline table query per app_id."""
        if not self.per_app_id_enabled:
            return

        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.init.sql"
        query_filename = f"{self.target_table_id}.query.sql"
        view_filename = f"{self.target_table_id[:-3]}.view.sql"
        view_metadata_filename = f"{self.target_table_id[:-3]}.metadata.yaml"
        table_metadata_filename = f"{self.target_table_id}.metadata.yaml"

        table = tables[f"{self.prefix}_table"]
        view = tables[f"{self.prefix}_view"]
        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=tables["daily_table"].split(".")[-2],
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        query_sql = render(query_filename, template_folder=PATH, **render_kwargs)
        view_sql = render(view_filename, template_folder=PATH, **render_kwargs)
        view_metadata = render(
            view_metadata_filename, template_folder=PATH, format=False, **render_kwargs
        )
        table_metadata = render(
            table_metadata_filename, template_folder=PATH, format=False, **render_kwargs
        )

        if not self.no_init:
            try:
                init_sql = render(init_filename, template_folder=PATH, **render_kwargs)
            except TemplateNotFound:
                init_sql = render(
                    query_filename, template_folder=PATH, init=True, **render_kwargs
                )

        if not (referenced_table_exists(view_sql)):
            logging.info("Skipping view for table which doesn't exist:" f" {table}")
            return

        if output_dir:
            write_sql(
                output_dir, view, "metadata.yaml", view_metadata, skip_existing=True
            )
            write_sql(output_dir, view, "view.sql", view_sql, skip_existing=True)
            write_sql(
                output_dir, table, "metadata.yaml", table_metadata, skip_existing=True
            )
            write_sql(output_dir, table, "query.sql", query_sql, skip_existing=True)

            if not self.no_init:
                write_sql(output_dir, table, "init.sql", init_sql, skip_existing=True)

            write_dataset_metadata(output_dir, view)

    def generate_per_app(self, project_id, app_info, output_dir=None):
        """Generate the baseline table query per app_name."""
        if not self.per_app_enabled:
            return

        target_view_name = "_".join(self.target_table_id.split("_")[:-1])
        target_dataset = app_info[0]["app_name"]

        datasets = [
            (a["bq_dataset_family"], a.get("app_channel", "release")) for a in app_info
        ]

        if len(datasets) == 1 and target_dataset == datasets[0][0]:
            # This app only has a single channel, and the app_name
            # exactly matches the generated bq_dataset_family, so
            # the existing per-app_id dataset also serves as the
            # per-app dataset, thus we don't have to provision
            # union views.
            if self.per_app_id_enabled:
                return

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{target_dataset}.{target_view_name}",
            datasets=datasets,
            table=target_view_name,
            target_table=f"{target_dataset}_derived.{self.target_table_id}",
            app_name=app_info[0]["app_name"],
        )
        render_kwargs.update(self.custom_render_kwargs)

        if self.cross_channel_template:
            sql = render(
                self.cross_channel_template, template_folder=PATH, **render_kwargs
            )
            view = f"{project_id}.{target_dataset}.{target_view_name}"

            if output_dir:
                write_dataset_metadata(output_dir, view)

            if not (referenced_table_exists(sql)):
                logging.info("Skipping view for table which doesn't exist:" f" {view}")
                return

            if output_dir:
                write_sql(output_dir, view, "view.sql", sql, skip_existing=True)
        else:
            query_filename = f"{target_view_name}.query.sql"
            query_sql = render(query_filename, template_folder=PATH, **render_kwargs)
            view_sql = render(
                f"{target_view_name}.view.sql", template_folder=PATH, **render_kwargs
            )
            metadata = render(
                f"{self.target_table_id[:-3]}.metadata.yaml",
                template_folder=PATH,
                format=False,
                **render_kwargs,
            )
            table = f"{project_id}.{target_dataset}_derived.{self.target_table_id}"
            view = f"{project_id}.{target_dataset}.{target_view_name}"

            if not (referenced_table_exists(query_sql)):
                logging.info(
                    "Skipping query for table which doesn't exist:"
                    f" {self.target_table_id}"
                )
                return

            if output_dir:
                write_sql(output_dir, table, "query.sql", query_sql, skip_existing=True)
                write_sql(
                    output_dir, table, "metadata.yaml", metadata, skip_existing=True
                )
                write_sql(output_dir, view, "view.sql", view_sql, skip_existing=True)
                write_dataset_metadata(output_dir, view)
                write_dataset_metadata(output_dir, table, derived_dataset_metadata=True)
