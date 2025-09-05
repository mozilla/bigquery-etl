"""Generate Events First Seen table."""

import os
from collections import namedtuple
from enum import Enum
from pathlib import Path

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import (
    GleanTable,
    get_table_dir,
    render,
    write_sql,
)

TARGET_TABLE = "events_first_seen"
VERSION = "v1"
PREFIX = "events_first_seen"
PATH = Path(os.path.dirname(__file__))


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    firefox_desktop = "Firefox Desktop"


class EventsFirstSeenTable(GleanTable):
    """Represents generated events_first_seen table."""

    def __init__(self) -> None:
        """Initialize events_first_seen table."""
        self.per_app_id_enabled = False
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = f"{TARGET_TABLE}_{VERSION}"
        self.custom_render_kwargs = {}
        self.base_table_name = "events_stream_v1"

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        # output_dir = Path(output_dir)
        browser = Browsers["firefox_desktop"]

        # Include only selected apps to avoid too complex query
        include_apps = ConfigLoader.get(
            "generate", "glean_usage", "events_first_seen", "include_apps", fallback=[]
        )

        apps = [app[0] for app in apps if app[0]["app_name"] in include_apps]

        render_kwargs = dict(
            project_id=project_id,
            output_dir=Path(output_dir),
            base_table=self.base_table_name,
            app_name=browser.name,
            events_first_seen_table=f"{TARGET_TABLE}_{VERSION}",
            events_first_seen_view=f"{TARGET_TABLE}",
            apps=apps,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        # firefox_desktop_derived/events_first_seen_v1/query.sql
        query_filename = f"{TARGET_TABLE}_{VERSION}.query.sql"

        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )

        # firefox_desktop_derived/events_first_seen_v1/metadata.yaml
        metadatav1_filename = f"{TARGET_TABLE}_{VERSION}.metadata.yaml"

        metadatav1 = render(
            sql_filename=metadatav1_filename,
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )

        # firefox_desktop_derived/events_first_seen_v1/schema.yaml
        schema_filename = f"{TARGET_TABLE}_{VERSION}.schema.yaml"

        schema = render(
            sql_filename=schema_filename,
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )

        # firefox_desktop/events_first_seen/view.sql
        view_filename = f"{TARGET_TABLE}.view.sql"

        view_sql = render(
            sql_filename=view_filename,
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )

        # firefox_desktop_derived/events_first_seen_v1/metadata.yaml
        metadata_view_filename = f"{TARGET_TABLE}.metadata.yaml"

        metadata_view = render(
            sql_filename=metadata_view_filename,
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )

        table = f"{project_id}.{browser.name}_derived.{TARGET_TABLE}_{VERSION}"

        view = f"{project_id}.{browser.name}.{TARGET_TABLE}"

        if output_dir:
            artifacts = [
                Artifact(table, "metadata.yaml", metadatav1),
                Artifact(table, "query.sql", query_sql),
                Artifact(table, "schema.yaml", schema),
                Artifact(view, "view.sql", view_sql),
                Artifact(view, "metadata.yaml", metadata_view),
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
