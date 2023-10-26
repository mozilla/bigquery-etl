"""Generate Materialized Views for event monitoring."""

from sql_generators.glean_usage.common import (
    GleanTable,
    table_names_from_baseline,
    render,
    get_table_dir,
    write_sql,
)
from pathlib import Path
import os
from jinja2 import TemplateNotFound
from collections import namedtuple


TARGET_TABLE_ID = "event_monitoring_live_v1"
PREFIX = "event_monitoring_live"
PATH = Path(os.path.dirname(__file__))


class EventMonitoringMaterializedView(GleanTable):
    """Represents the generated materialized view for event monitoring."""

    def __init__(self) -> None:
        """Initialize materialized view generation."""
        self.no_init = False
        self.per_app_id_enabled = True
        self.per_app_enabled = True
        self.prefix = PREFIX
        self.target_table_id = TARGET_TABLE_ID
        self.cross_channel_template = "cross_channel_event_monitoring.view.sql"

    def generate_per_app_id(
        self, project_id, baseline_table, output_dir=None, use_cloud_function=True
    ):
        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.init.sql"
        metadata_filename = f"{self.target_table_id}.metadata.yaml"

        table = tables[f"{self.prefix}_table"]

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=tables["event_monitoring_live"].split(".")[-2],
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        # generated files to update
        Artifact = namedtuple("Artifact", "table_id basename sql")
        artifacts = []

        if not self.no_init:
            try:
                init_sql = render(
                    init_filename, template_folder=PATH / "templates", **render_kwargs
                )
            except TemplateNotFound:
                init_sql = render(
                    init_filename,
                    template_folder=PATH / "templates",
                    init=True,
                    **render_kwargs,
                )
                artifacts.append(Artifact(table, "init.sql", init_sql))

                metadata = render(
                    metadata_filename,
                    template_folder=PATH / "templates",
                    format=False,
                    **render_kwargs,
                )
                artifacts.append(Artifact(table, "metadata.yaml", metadata))

        skip_existing_artifact = self.skip_existing(output_dir, project_id)

        if output_dir:
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
