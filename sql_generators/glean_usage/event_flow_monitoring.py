"""Generate Aggregate table for monitoring event flows."""

import os
from collections import namedtuple
from pathlib import Path

from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import (
    GleanTable,
    get_table_dir,
    render,
    write_sql,
)

AGGREGATE_TABLE_NAME = "event_flow_monitoring_aggregates_v1"
TARGET_DATASET_CROSS_APP = "monitoring"
PREFIX = "event_flow_monitoring"
PATH = Path(os.path.dirname(__file__))


class EventFlowMonitoring(GleanTable):
    """Represents the generated aggregated table for event flow monitoring."""

    def __init__(self) -> None:
        self.no_init = False
        self.per_app_id_enabled = False
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = AGGREGATE_TABLE_NAME
        self.custom_render_kwargs = {}
        self.base_table_name = "events_unnested"

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        apps = [app[0] for app in apps]

        render_kwargs = dict(
            project_id=project_id,
            target_table=f"{TARGET_DATASET_CROSS_APP}_derived.{AGGREGATE_TABLE_NAME}",
            apps=apps,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"{AGGREGATE_TABLE_NAME}.script.sql"
        script_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            f"{AGGREGATE_TABLE_NAME}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        schema = render(
            f"{AGGREGATE_TABLE_NAME}.schema.yaml",
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
                Artifact(table, "script.sql", script_sql),
                Artifact(table, "schema.yaml", schema)
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
