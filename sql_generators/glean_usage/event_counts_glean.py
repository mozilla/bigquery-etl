"""Generate Materialized Views and aggregate queries for event monitoring."""

import os
from collections import OrderedDict, namedtuple
from pathlib import Path

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import (
    GleanTable,
    get_table_dir,
    render,
    write_sql,
    get_prod_datasets_with_event,
    get_tables_with_events,
)

TARGET_TABLE_ID = "event_counts_glean_v2"
TARGET_DATASET_CROSS_APP = "monitoring"
PREFIX = "event_counts_glean"
PATH = Path(os.path.dirname(__file__))


class EventCounts(GleanTable):
    """Represents the generated materialized view for event monitoring."""

    def __init__(self) -> None:
        """Initialize event_counts generation."""
        self.per_app_id_enabled = False
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = TARGET_TABLE_ID
        self.common_render_kwargs = {}
        self.base_table_name = "events_v1"

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
    ):
        """Generate a query across all apps."""
        target_view_name = "_".join(self.target_table_id.split("_")[:-1])

        # reuse event_monitoring settings
        events_table_overwrites = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "events_tables", fallback={}
        )

        event_tables_per_dataset = OrderedDict()

        # Skip any not-allowed app.
        skip_apps = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "skip_apps", fallback=[]
        )
        skip_app_ids = ConfigLoader.get(
            "generate",
            "glean_usage",
            "events_monitoring",
            "skip_app_ids",
            fallback=[],
        )

        for app_name, app_ids_info in apps.items():
            for app_dataset in app_ids_info:
                if (
                    app_name in skip_apps
                    or app_dataset["app_id"] in skip_app_ids
                    or app_dataset.get("deprecated", False) is True
                ):
                    continue

                dataset = app_dataset["bq_dataset_family"]

                if app_name in events_table_overwrites:
                    event_tables_per_dataset[dataset] = events_table_overwrites[
                        app_name
                    ]
                else:
                    v1_name = app_dataset["v1_name"]
                    event_tables = [
                        f"{ping.replace('-', '_')}_v1"
                        for ping in get_tables_with_events(
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
            target_table=f"{TARGET_DATASET_CROSS_APP}_derived.{self.target_table_id}",
            apps=list(apps.values()),
            prod_datasets=get_prod_datasets_with_event(),
            event_tables_per_dataset=event_tables_per_dataset,
        )
        render_kwargs.update(self.common_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"{self.target_table_id}.query.sql"
        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            f"{self.target_table_id}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        table = (
            f"{project_id}.{TARGET_DATASET_CROSS_APP}_derived.{self.target_table_id}"
        )
        schema = (
            PATH / "templates" / f"{self.target_table_id}.schema.yaml"
        ).read_text()

        if output_dir:
            artifacts = [
                Artifact(table, "metadata.yaml", metadata),
                Artifact(table, "query.sql", query_sql),
                Artifact(table, "schema.yaml", schema),
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
