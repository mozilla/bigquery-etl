"""Generating and run baseline_clients_daily queries for Glean apps."""
from bigquery_etl.glean_usage.common import GleanTable

BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
PREFIX = "daily"


class BaselineClientsDailyTable(GleanTable):
    def __init__(self):
        self.target_table_id = BASELINE_DAILY_TABLE_ID
        self.prefix = PREFIX
        self.custom_render_kwargs = {}
        self.no_init = False
