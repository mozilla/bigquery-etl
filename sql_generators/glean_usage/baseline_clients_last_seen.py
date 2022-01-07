"""Generate and run baseline_clients_last_seen queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "baseline_clients_last_seen_v1"
PREFIX = "last_seen"
USAGE_TYPES = ("seen", "created_profile", "seen_session_start", "seen_session_end")


class BaselineClientsLastSeenTable(GleanTable):
    """Represents generated baseline_clients_last_seen table."""

    def __init__(self):
        """Initialize baseline_clients_last_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.custom_render_kwargs = dict(
            usage_types=USAGE_TYPES,
        )
        self.no_init = False
