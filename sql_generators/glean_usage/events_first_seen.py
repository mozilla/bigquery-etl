"""Generate events first seen queries for Glean apps."""

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "events_first_seen_v1"
PREFIX = "events_first_seen"


class EventsFirstSeenTable(GleanTable):
    """Represents generated events_first_seen table."""

    def __init__(self):
        """Initialize events_first_seen table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.per_app_enabled = True
        self.per_app_id_enabled = True
        self.across_apps_enabled = False
        self.cross_channel_template = "cross_channel.view.sql"
        self.base_table_name = "events_v1"
        self.common_render_kwargs = {}

    def generate_per_app_id(
        self,
        project_id,
        baseline_table,
        app_name,
        app_id_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
        id_token=None,
    ):
        """Generate the events_first_seen table query per app_id."""
        # Include only selected apps
        if app_name in ConfigLoader.get(
            "generate", "glean_usage", "events_first_seen", "include_apps", fallback=[]
        ):
            super().generate_per_app_id(
                project_id,
                baseline_table,
                app_name,
                app_id_info,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
                parallelism=parallelism,
                id_token=id_token,
            )

    def generate_per_app(
        self,
        project_id,
        app_name,
        app_ids_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
        id_token=None,
        all_base_tables_exist=None,
    ):
        """Generate the events_first_seen table query per app_name."""
        if app_name in ConfigLoader.get(
            "generate", "glean_usage", "events_first_seen", "include_apps", fallback=[]
        ):
            super().generate_per_app(
                project_id,
                app_name,
                app_ids_info,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
                parallelism=parallelism,
                id_token=id_token,
                all_base_tables_exist=all_base_tables_exist,
            )
