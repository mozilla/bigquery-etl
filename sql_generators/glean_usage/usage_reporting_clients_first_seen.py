"""Generating and run dau_reporting_clients_first_seen queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "dau_reporting_clients_first_seen_v1"
PREFIX = "dau_reporting_clients_first_seen"


class UsageReportingClientsFirstSeenTable(GleanTable):
    """Represents generated dau_reporting_clients_first_seen table."""

    def __init__(self):
        """Initialize dau_reporting_clients_first_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.base_table_name = "usage_reporting_v1"
