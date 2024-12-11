"""Generating and run usage_reporting_clients_daily queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "usage_reporting_clients_daily_v1"
PREFIX = "usage_reporting_clients_daily"


class UsageReportingClientsDailyTable(GleanTable):
    """Represents generated usage_reporting_clients_daily table."""

    def __init__(self):
        """Initialize usage_reporting_clients_daily table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.base_table_name = "usage_reporting_v1"
