"""Generate and run clients_last_seen_joined queries for Glean apps."""

from bigquery_etl.glean_usage.common import GleanTable

TARGET_TABLE_ID = "clients_last_seen_joined_v1"
PREFIX = "clients_last_seen_joined"


class ClientsLastSeenJoined(GleanTable):
    """Represents generated clients_last_seen_joined table."""

    def __init__(self):
        """Initialize clients_last_seen_joined table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.no_init = True
        self.per_app_id_enabled = False
        self.cross_channel_template = None
