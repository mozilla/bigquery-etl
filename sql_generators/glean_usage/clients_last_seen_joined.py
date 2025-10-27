"""Generate and run clients_last_seen_joined queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

from bigquery_etl.config import ConfigLoader

TARGET_TABLE_ID = "clients_last_seen_joined_v1"
PREFIX = "clients_last_seen_joined"


class ClientsLastSeenJoined(GleanTable):
    """Represents generated clients_last_seen_joined table."""

    def __init__(self):
        """Initialize clients_last_seen_joined table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.per_app_channel_enabled = False
        self.cross_channel_template = None
        self.per_app_requires_all_base_tables = True

    def generate_per_app(
        self,
        project_id,
        app_name,
        app_channels_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
        id_token=None,
        all_base_tables_exist=None,
    ):
        """Generate per-app datasets."""
        skip_apps = ConfigLoader.get(
            "generate", "glean_usage", "clients_last_seen_joined", "skip_apps", fallback=[]
        )
        if app_name in skip_apps:
            print(f"Skipping clients_last_seen_joined generation for {app_name}")
            return
        return super().generate_per_app(
            project_id,
            app_name,
            app_channels_info,
            output_dir,
            use_cloud_function,
            parallelism,
            id_token,
            all_base_tables_exist,
        )
