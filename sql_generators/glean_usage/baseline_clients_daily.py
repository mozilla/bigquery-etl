"""Generating and run baseline_clients_daily queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
PREFIX = "daily"


class BaselineClientsDailyTable(GleanTable):
    """Represents generated baseline_clients_daily table."""

    def __init__(self):
        """Initialize baseline_clients_daily table."""
        GleanTable.__init__(self)
        self.target_table_id = BASELINE_DAILY_TABLE_ID
        self.prefix = PREFIX
        self.common_render_kwargs = {}
        self.per_app_requires_all_base_tables = True
