"""Generate events stream queries for Glean apps."""

import re

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "events_stream_v1"
PREFIX = "events_stream"


class EventsStreamTable(GleanTable):
    """Represents generated events_stream table."""

    def __init__(self):
        """Initialize events_stream table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.no_init = True
        self.per_app_enabled = True
        self.per_app_id_enabled = True
        self.across_apps_enabled = False
        self.cross_channel_template = "cross_channel_events_stream.query.sql"
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

        super().generate_per_app_id(
            project_id, baseline_table, output_dir, use_cloud_function, app_info
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
