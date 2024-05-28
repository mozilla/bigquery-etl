"""Generate Aggregate table for monitoring event errors."""

import os
from collections import namedtuple
from pathlib import Path

from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from sql_generators.glean_usage.common import (
    GleanTable,
    get_table_dir,
    render,
    write_sql,
)

AGGREGATE_TABLE_NAME = "event_error_monitoring_aggregates_v1"
TARGET_DATASET_CROSS_APP = "monitoring"
PREFIX = "event_error_monitoring"
PATH = Path(os.path.dirname(__file__))


class EventErrorMonitoring(GleanTable):
    """Represents the generated aggregated table for event error monitoring."""

    def __init__(self) -> None:
        self.per_app_id_enabled = False
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = AGGREGATE_TABLE_NAME
        self.custom_render_kwargs = {}
        self.base_table_name = "events_v1"

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        prod_datasets_with_event = [
            s.bq_dataset_family
            for s in get_stable_table_schemas()
            if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
            and s.bq_table == "events_v1"
        ]

        default_events_table = ConfigLoader.get(
            "generate",
            "glean_usage",
            "events_monitoring",
            "default_event_table",
            fallback="events_v1",
        )
        events_table_overwrites = ConfigLoader.get(
            "generate", "glean_usage", "events_monitoring", "event_table", fallback={}
        )

        render_kwargs = dict(
            project_id=project_id,
            target_table=f"{TARGET_DATASET_CROSS_APP}_derived.{AGGREGATE_TABLE_NAME}",
            apps=apps,
            prod_datasets=prod_datasets_with_event,
            default_events_table=default_events_table,
            events_table_overwrites=events_table_overwrites,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"{AGGREGATE_TABLE_NAME}.query.sql"
        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            f"{AGGREGATE_TABLE_NAME}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        table = (
            f"{project_id}.{TARGET_DATASET_CROSS_APP}_derived.{AGGREGATE_TABLE_NAME}"
        )

        if output_dir:
            artifacts = [
                Artifact(table, "metadata.yaml", metadata),
                Artifact(table, "query.sql", query_sql),
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
