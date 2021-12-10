"""Generate unnested events queries for Glean apps."""

from common import GleanTable

TARGET_TABLE_ID = "events_unnested_v1"
PREFIX = "events_unnested"


class EventsUnnestedTable(GleanTable):
    """Represents generated events_unnested table."""

    def __init__(self):
        """Initialize events_unnested table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.no_init = True
        self.per_app_id_enabled = False
        self.cross_channel_template = "cross_channel_events_unnested.view.sql"
