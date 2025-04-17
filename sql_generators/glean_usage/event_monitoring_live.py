"""Generate Materialized Views and aggregate queries for event monitoring."""

import os
import re

from collections import namedtuple, OrderedDict
from datetime import datetime
from pathlib import Path
from typing import List, Set

import requests

from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from sql_generators.glean_usage.common import (
    GleanTable,
    get_app_info,
    get_table_dir,
    render,
    table_names_from_baseline,
    write_sql,
)

TARGET_TABLE_ID = "event_monitoring_live_v1"
TARGET_DATASET_CROSS_APP = "monitoring"
PREFIX = "event_monitoring"
PATH = Path(os.path.dirname(__file__))
GLEAN_APP_BASE_URL = "https://probeinfo.telemetry.mozilla.org/glean/{app_name}"
METRICS_INFO_URL = f"{GLEAN_APP_BASE_URL}/metrics"
PING_INFO_URL = f"{GLEAN_APP_BASE_URL}/pings"


class EventMonitoringLive(GleanTable):
    """Represents the generated materialized view for event monitoring."""

    def __init__(self) -> None:
        """Initialize materialized view generation."""
        self.per_app_id_enabled = True
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = TARGET_TABLE_ID
        self.custom_render_kwargs = {}
        self.base_table_name = "events_v1"

    def _get_prod_datasets_with_event(self) -> List[str]:
        """Get glean datasets with an events table in generated schemas."""
        return [
            s.bq_dataset_family
            for s in get_stable_table_schemas()
            if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
            and s.bq_table == "events_v1"
        ]

    def _get_tables_with_events(
        self, v1_name: str, bq_dataset_name: str, skip_min_ping: bool
    ) -> Set[str]:
        """Get tables for the given app that receive event type metrics."""
        pings = set()
        metrics_resp = requests.get(METRICS_INFO_URL.format(app_name=v1_name))
        metrics_resp.raise_for_status()
        metrics_json = metrics_resp.json()

        min_pings = set()
        if skip_min_ping:
            ping_resp = requests.get(PING_INFO_URL.format(app_name=v1_name))
            ping_resp.raise_for_status()
            ping_json = ping_resp.json()
            min_pings = {
                name
                for name, info in ping_json.items()
                if not info["history"][-1].get("include_info_sections", True)
            }

        for _, metric in metrics_json.items():
            if metric.get("type", None) == "event":
                latest_history = metric.get("history", [])[-1]
                pings.update(latest_history.get("send_in_pings", []))

        if bq_dataset_name in self._get_prod_datasets_with_event():
            pings.add("events")

        pings = pings.difference(min_pings)

        return pings

    def generate_per_app_id(
        self,
        project_id,
        baseline_table,
        output_dir=None,
        use_cloud_function=True,
        app_info=[],
        parallelism=8,
        id_token=None,
    ):
        # Get the app ID from the baseline_table name.
        # This is what `common.py` also does.
        app_id = re.sub(r"_stable\..+", "", baseline_table)
        app_id = ".".join(app_id.split(".")[1:])

        # Skip any not-allowed app.
        if app_id in ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "skip_apps", fallback=[]
        ):
            return

        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.materialized_view.sql"
        metadata_filename = f"{self.target_table_id}.metadata.yaml"

        table = tables[f"{self.prefix}"]
        dataset = tables[self.prefix].split(".")[-2].replace("_derived", "")

        events_table_overwrites = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "events_tables", fallback={}
        )

        app_name = [
            app_dataset["app_name"]
            for _, app in get_app_info().items()
            for app_dataset in app
            if dataset == app_dataset["bq_dataset_family"]
        ][0]

        if app_name in events_table_overwrites:
            events_tables = events_table_overwrites[app_name]
        else:
            v1_name = [
                app_dataset["v1_name"]
                for _, app in get_app_info().items()
                for app_dataset in app
                if dataset == app_dataset["bq_dataset_family"]
            ][0]
            events_tables = self._get_tables_with_events(
                v1_name, dataset, skip_min_ping=True
            )
            events_tables = [
                f"{ping.replace('-', '_')}_v1"
                for ping in events_tables
                if ping
                not in ConfigLoader.get(
                    "generate", "glean_usage", "events_monitoring", "skip_pings"
                )
            ]

        if len(events_tables) == 0:
            return

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=tables[self.prefix].split(".")[-2],
            dataset=dataset,
            current_date=datetime.today().strftime("%Y-%m-%d"),
            app_name=[
                app_dataset["canonical_app_name"]
                for _, app in get_app_info().items()
                for app_dataset in app
                if dataset == app_dataset["bq_dataset_family"]
            ][0],
            events_tables=sorted(events_tables),
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        # generated files to update
        Artifact = namedtuple("Artifact", "table_id basename sql")
        artifacts = []

        init_sql = render(
            init_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            metadata_filename,
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        artifacts.append(Artifact(table, "metadata.yaml", metadata))

        skip_existing_artifact = self.skip_existing(output_dir, project_id)

        if output_dir:
            artifacts.append(Artifact(table, "materialized_view.sql", init_sql))

            for artifact in artifacts:
                destination = (
                    get_table_dir(output_dir, artifact.table_id) / artifact.basename
                )
                skip_existing = str(destination) in skip_existing_artifact

                write_sql(
                    output_dir,
                    artifact.table_id,
                    artifact.basename,
                    artifact.sql,
                    skip_existing=skip_existing,
                )

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        aggregate_table = "event_monitoring_aggregates_v1"
        target_view_name = "_".join(self.target_table_id.split("_")[:-1])

        events_table_overwrites = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "events_tables", fallback={}
        )

        event_tables_per_dataset = OrderedDict()

        # Skip any not-allowed app.
        skip_apps = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "skip_apps", fallback=[]
        )

        for app in apps:
            for app_dataset in app:
                if app_dataset["app_name"] in skip_apps:
                    continue

                dataset = app_dataset["bq_dataset_family"]
                app_name = [
                    app_dataset["app_name"]
                    for _, app in get_app_info().items()
                    for app_dataset in app
                    if dataset == app_dataset["bq_dataset_family"]
                ][0]

                if app_name in events_table_overwrites:
                    event_tables_per_dataset[dataset] = events_table_overwrites[
                        app_name
                    ]
                else:
                    v1_name = [
                        app_dataset["v1_name"]
                        for _, app in get_app_info().items()
                        for app_dataset in app
                        if dataset == app_dataset["bq_dataset_family"]
                    ][0]
                    event_tables = [
                        f"{ping.replace('-', '_')}_v1"
                        for ping in self._get_tables_with_events(
                            v1_name,
                            app_dataset["bq_dataset_family"],
                            skip_min_ping=True,
                        )
                        if ping
                        not in ConfigLoader.get(
                            "generate", "glean_usage", "events_monitoring", "skip_pings"
                        )
                    ]

                    if len(event_tables) > 0:
                        event_tables_per_dataset[dataset] = sorted(event_tables)

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{TARGET_DATASET_CROSS_APP}.{target_view_name}",
            table=target_view_name,
            target_table=f"{TARGET_DATASET_CROSS_APP}_derived.{aggregate_table}",
            apps=apps,
            prod_datasets=self._get_prod_datasets_with_event(),
            event_tables_per_dataset=event_tables_per_dataset,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"{aggregate_table}.query.sql"
        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            f"{aggregate_table}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        table = f"{project_id}.{TARGET_DATASET_CROSS_APP}_derived.{aggregate_table}"

        view_sql = render(
            "event_monitoring_live.view.sql",
            template_folder=PATH / "templates",
            **render_kwargs,
        )
        view_metadata = render(
            "event_monitoring_live.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        schema = (
            PATH / "templates" / "event_monitoring_aggregates_v1.schema.yaml"
        ).read_text()

        view = f"{project_id}.{TARGET_DATASET_CROSS_APP}.{target_view_name}"
        if output_dir:
            artifacts = [
                Artifact(table, "metadata.yaml", metadata),
                Artifact(table, "query.sql", query_sql),
                Artifact(table, "schema.yaml", schema),
                Artifact(view, "metadata.yaml", view_metadata),
                Artifact(view, "view.sql", view_sql),
            ]

            for artifact in artifacts:
                destination = (
                    get_table_dir(output_dir, artifact.table_id) / artifact.basename
                )
                skip_existing = destination in skip_existing_artifacts

                write_sql(
                    output_dir,
                    artifact.table_id,
                    artifact.basename,
                    artifact.sql,
                    skip_existing=skip_existing,
                )
