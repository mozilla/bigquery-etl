"""Generate and run clients_daily_metrics queries for Glean apps."""

from bigquery_etl.glean_usage.common import GleanTable

TARGET_TABLE_ID = "clients_metrics_v1"
PREFIX = "clients_daily_metrics"

class ClientsDailyMetrics(GleanTable):
    """Represents generated clients_daily_metrics table."""

    def __init__(self):
        """Initialize clients_daily_metrics table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.no_init = True
        self.per_app_id_enabled = False
        self.cross_channel_template = "cross_channel_clients_daily_metrics.query.sql"
