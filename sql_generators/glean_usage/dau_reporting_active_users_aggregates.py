"""Generating and run dau_reporting_active_users_aggregates queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "dau_reporting_active_users_aggregates_v1"
PREFIX = "dau_reporting_active_users_aggregates"


class DauReportingActiveUsersAggregatesTable(GleanTable):
    """Represents generated dau_reporting_active_users_aggregates table."""

    def __init__(self):
        """Initialize dau_reporting_active_users_aggregates table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.base_table_name = "dau_reporting_v1"
