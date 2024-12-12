"""Generating and run usage_reporting_active_users_aggregates queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "usage_reporting_active_users_aggregates_v1"
PREFIX = "usage_reporting_active_users_aggregates"


class UsageReportingActiveUsersAggregatesTable(GleanTable):
    """Represents generated usage_reporting_active_users_aggregates table."""

    def __init__(self):
        """Initialize usage_reporting_active_users_aggregates table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.base_table_name = "usage_reporting_v1"
