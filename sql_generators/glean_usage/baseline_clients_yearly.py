"""Generate and run baseline_clients_yearly queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "baseline_clients_yearly_v1"
PREFIX = "clients_yearly"
USAGE_TYPES = [
    # (Field usage type, field condition from baseline_clients_daily)
    ("seen", "TRUE"),
    ("active", "durations > 0"),
]


class BaselineClientsYearlyTable(GleanTable):
    """Represents generated baseline_clients_yearly table."""

    def __init__(self):
        """Initialize baseline_clients_yearly table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.custom_render_kwargs = dict(
            usage_types=USAGE_TYPES,
        )
        self.no_init = False
