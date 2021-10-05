"""Generate and run clients_last_seen_metrics queries for Glean apps."""

import os
from pathlib import Path

import yaml

from bigquery_etl.glean_usage.common import GleanTable

TARGET_TABLE_ID = "clients_last_seen_metrics_v1"
PREFIX = "clients_last_seen_metrics"


class ClientsLastSeenMetrics(GleanTable):
    """Represents generated clients_last_seen_metrics table."""

    def __init__(self):
        """Initialize clients_last_seen_metrics table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.no_init = True
        self.per_app_id_enabled = False
        self.cross_channel_template = None

        with open(
            Path(os.path.dirname(__file__)) / "templates" / "metrics_templating.yaml",
            "r",
        ) as f:
            metrics_config = yaml.safe_load(f) or {}
            self.custom_render_kwargs = {"metrics": metrics_config}
