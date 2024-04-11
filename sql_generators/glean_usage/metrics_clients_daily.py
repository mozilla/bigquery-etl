"""Generate and run metrics_clients_daily queries for Glean apps."""

import os
from pathlib import Path

import yaml

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "metrics_clients_daily_v1"
PREFIX = "metrics_clients_daily"


class MetricsClientsDaily(GleanTable):
    """Represents generated metrics_clients_daily table."""

    def __init__(self):
        """Initialize metrics_clients_daily table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.per_app_id_enabled = False
        self.cross_channel_template = None

        with open(
            Path(os.path.dirname(__file__)) / "templates" / "metrics_templating.yaml",
            "r",
        ) as f:
            metrics_config = yaml.safe_load(f) or {}
            self.custom_render_kwargs = {"metrics": metrics_config}
