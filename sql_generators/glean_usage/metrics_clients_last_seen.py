"""Generate and run metrics_clients_last_seen queries for Glean apps."""

import os
from pathlib import Path

import yaml

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "metrics_clients_last_seen_v1"
PREFIX = "metrics_clients_last_seen"


class MetricsClientsLastSeen(GleanTable):
    """Represents generated metrics_clients_last_seen table."""

    def __init__(self):
        """Initialize metrics_clients_last_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.per_app_id_enabled = False
        self.cross_channel_template = None
        self.per_app_requires_all_base_tables = True

        with open(
            Path(os.path.dirname(__file__)) / "templates" / "metrics_templating.yaml",
            "r",
        ) as f:
            metrics_config = yaml.safe_load(f) or {}
            self.custom_render_kwargs = {"metrics": metrics_config}

    def generate_per_app(
        self,
        project_id,
        app_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
        id_token=None,
        all_base_tables_exist=None,
    ):
        """Generate per-app datasets."""
        skip_apps = ConfigLoader.get(
            "generate",
            "glean_usage",
            "metrics_clients_last_seen",
            "skip_apps",
            fallback=[],
        )
        if app_info[0]["app_name"] in skip_apps:
            print(
                f"Skipping metrics_clients_last_seen generation for {app_info[0]['app_name']}"
            )
            return
        return super().generate_per_app(
            project_id,
            app_info,
            output_dir,
            use_cloud_function,
            parallelism,
            id_token,
            all_base_tables_exist,
        )
