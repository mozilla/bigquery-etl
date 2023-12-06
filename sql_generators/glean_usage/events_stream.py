"""Generate events stream queries for Glean apps."""

import os
import re
from collections import namedtuple
from pathlib import Path

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import (
    GleanTable,
    get_table_dir,
    render,
    table_names_from_baseline,
    write_sql,
)

TARGET_TABLE_ID = "events_stream_live_v1"
PREFIX = "events_stream"
PATH = Path(os.path.dirname(__file__))


class EventsStreamTable(GleanTable):
    """Represents generated events_stream table."""

    def __init__(self):
        """Initialize events_stream table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.no_init = False
        self.per_app_enabled = True
        self.per_app_id_enabled = True
        self.across_apps_enabled = False
        self.cross_channel_template = "cross_channel_events_stream.view.sql"
        self.base_table_name = "events_v1"

    def generate_per_app_id(
        self,
        project_id,
        baseline_table,
        output_dir=None,
        use_cloud_function=True,
        app_info=[],
    ):
        # Get the app ID from the baseline_table name.
        # This is what `common.py` also does.
        app_id = re.sub(r"_stable\..+", "", baseline_table)
        app_id = ".".join(app_id.split(".")[1:])

        # Skip any not-allowed app.
        if app_id not in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "app_ids", fallback=[]
        ):
            return

        tables = table_names_from_baseline(baseline_table, include_project_id=False)
        init_filename = f"{self.target_table_id}.init.sql"
        view_filename = f"{self.target_table_id[:-3]}.view.sql"
        view_metadata_filename = f"{self.target_table_id[:-3]}.metadata.yaml"
        metadata_filename = f"{self.target_table_id}.metadata.yaml"
        table = tables[f"{self.prefix}_table"]
        view = tables[f"{self.prefix}_view"]

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=f"{app_id}_derived",
            dataset=app_id,
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        # generated files to update
        Artifact = namedtuple("Artifact", "table_id basename sql")
        artifacts = []

        if not self.no_init:
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

            view_sql = render(
                view_filename, template_folder=PATH / "templates", **render_kwargs
            )
            artifacts.append(Artifact(view, "view.sql", view_sql))
            view_metadata = render(
                view_metadata_filename,
                template_folder=PATH / "templates",
                format=False,
            )
            artifacts.append(Artifact(view, "metadata.yaml", view_metadata))

        skip_existing_artifact = self.skip_existing(output_dir, project_id)

        if output_dir:
            if not self.no_init:
                artifacts.append(Artifact(table, "init.sql", init_sql))

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

    def generate_per_app(
        self, project_id, app_info, output_dir=None, use_cloud_function=True
    ):
        """Generate the events_stream table query per app_name."""
        target_dataset = app_info[0]["app_name"]
        if target_dataset in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "datasets", fallback=[]
        ):
            super().generate_per_app(project_id, app_info, output_dir)
