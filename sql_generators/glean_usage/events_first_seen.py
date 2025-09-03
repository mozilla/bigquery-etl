"""Generate Events First Seen table."""

import os
from enum import Enum
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql
from sql_generators.glean_usage.common import GleanTable

project_id = "moz-fx-data-shared-prod"
BASE_TABLE = "events_stream_v1"
TARGET_TABLE = "events_first_seen"
VERSION = "v1"
PREFIX = "events_first_seen"
PATH = Path(os.path.dirname(__file__))


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    firefox_desktop = "Firefox Desktop"


class EventsFirstSeenTable(GleanTable):
    """Represents generated events_first_seen table."""

    def __init__(self):
        """Initialize events_first_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = f"{TARGET_TABLE}_{VERSION}"
        self.prefix = PREFIX
        self.custom_render_kwargs = {}

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
    ):
        """Generate events_first_seen table.

        The parent folders will be created if not existing and existing files will be overwritten.
        """
        env = Environment(loader=FileSystemLoader(str(PATH / "templates")))
        output_dir = Path(output_dir)
        browser = Browsers["firefox_desktop"]

        # firefox_desktop_derived/events_first_seen_v1/query.sql
        query_template = env.get_template(f"{TARGET_TABLE}_{VERSION}.query.sql")

        query_sql = reformat(
            query_template.render(
                project_id=project_id,
                app_name=browser.name,
                base_table=BASE_TABLE,
                events_first_seen_table=f"{TARGET_TABLE}_{VERSION}",
            )
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{project_id}.{browser.name}_derived.{TARGET_TABLE}_{VERSION}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        # firefox_desktop_derived/events_first_seen_v1/metadata.yaml
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{project_id}.{browser.name}_derived.{TARGET_TABLE}_v1",
            basename="metadata.yaml",
            sql=render(
                sql_filename=f"{TARGET_TABLE}_{VERSION}.metadata.yaml",
                template_folder=PATH / "templates",
                output_dir=output_dir,
                app_name=browser.name,
                events_first_seen_table=f"{TARGET_TABLE}_{VERSION}",
                format=False,
            ),
            skip_existing=False,
        )

        # firefox_desktop/events_first_seen/view.sql
        view_template = env.get_template(f"{TARGET_TABLE}.view.sql")

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{project_id}.{browser.name}.{TARGET_TABLE}",
            basename="view.sql",
            sql=reformat(
                view_template.render(
                    project_id=project_id,
                    app_name=browser.name,
                    events_first_seen_view=TARGET_TABLE,
                    events_first_seen_table=f"{TARGET_TABLE}_{VERSION}",
                )
            ),
            skip_existing=False,
        )

        # firefox_desktop/events_first_seen/metadata.yaml
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{project_id}.{browser.name}.{TARGET_TABLE}",
            basename="metadata.yaml",
            sql=render(
                sql_filename=f"{TARGET_TABLE}.metadata.yaml",
                template_folder=PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )
