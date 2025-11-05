"""Generate telemetry health ping latency query."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "telemetry_health_ping_latency_v1"
PREFIX = "telemetry_health"


class TelemetryHealthPingLatencyTable(GleanTable):
    """Represents generated telemetry health ping latency table."""

    def __init__(self):
        """Initialize telemetry health ping latency table."""
        super().__init__()
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.template = "telemetry_health_ping_latency_v1.query.sql"
        self.per_app_id_enabled = False
        self.per_app_enabled = True
        self.cross_channel_template = None
        self.per_app_requires_all_base_tables = True
