"""Generate unnested events queries for Glean apps."""

from sql_generators.glean_usage.common import GleanTable

TARGET_TABLE_ID = "events_unnested_v1"
PREFIX = "events_unnested"
DATASET_SKIP = {
    # VPN events are in main pings
    "mozilla_vpn",
    "mozilla_vpn_android",
}


class EventsUnnestedTable(GleanTable):
    """Represents generated events_unnested table."""

    def __init__(self):
        """Initialize events_unnested table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.no_init = True
        self.per_app_id_enabled = False
        self.cross_channel_template = "cross_channel_events_unnested.view.sql"

    def generate_per_app(
        self,
        project_id,
        app_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
    ):
        """Generate the events_unnested table query per app_name."""
        target_dataset = app_info[0]["app_name"]
        if target_dataset not in DATASET_SKIP:
            super().generate_per_app(project_id, app_info, output_dir)
