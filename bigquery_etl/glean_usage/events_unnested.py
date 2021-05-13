"""Generate unnested events queries for Glean apps."""

import logging

from bigquery_etl.glean_usage.common import GleanTable

TARGET_TABLE_ID = "events_unnested_v1"
PREFIX = "events_unnested"


class EventsUnnestedTable(GleanTable):
    """Represents generated events_unnested table."""

    def __init__(self):
        """Initialize events_unnested table."""
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.custom_render_kwargs = {}
        self.no_init = False
        self.cross_channel_template = "cross_channel_events_unnested.view.sql"

    def generate_per_app_id(self, project_id, baseline_table, output_dir=None):
        """Generate the baseline table query per app_name."""
        logging.info("generate_per_app_id() not implemented for EventsUnnestedTable")
