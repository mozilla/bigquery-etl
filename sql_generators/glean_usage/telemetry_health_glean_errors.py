"""Generate telemetry health glean errors query."""

from sql_generators.glean_usage.common import GleanTable

# module-level constants (follow project convention used elsewhere)
TARGET_TABLE_ID = "telemetry_health_glean_errors_v1"
PREFIX = "telemetry_health"


class TelemetryHealthGleanErrorsTable(GleanTable):
    """Represents generated telemetry health glean errors table."""

    def __init__(self):
        """Initialize telemetry health glean errors table."""
        super().__init__()
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.template = "telemetry_health_glean_errors_v1.query.sql"
        self.per_app_id_enabled = False
        self.per_app_enabled = True
        self.base_table_name = "metrics_v1"
        self.cross_channel_template = None
        self.per_app_requires_all_base_tables = True
