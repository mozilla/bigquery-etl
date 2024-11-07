"""Generate events stream queries for Glean apps."""

import re

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import GleanTable, ping_has_metrics

TARGET_TABLE_ID = "events_stream_v1"
PREFIX = "events_stream"


class EventsStreamTable(GleanTable):
    """Represents generated events_stream table."""

    def __init__(self):
        """Initialize events_stream table."""
        GleanTable.__init__(self)
        self.target_table_id = TARGET_TABLE_ID
        self.prefix = PREFIX
        self.per_app_enabled = True
        self.per_app_id_enabled = True
        self.across_apps_enabled = True
        self.cross_channel_template = "cross_channel_events_stream.query.sql"
        self.base_table_name = "events_v1"
        self.custom_render_kwargs = {}

    def generate_per_app_id(
        self,
        project_id,
        baseline_table,
        output_dir=None,
        use_cloud_function=True,
        app_info=[],
        parallelism=8,
        id_token=None,
    ):
        # Get the app ID from the baseline_table name.
        # This is what `common.py` also does.
        app_id = re.sub(r"_stable\..+", "", baseline_table)
        app_id = ".".join(app_id.split(".")[1:])

        # Skip any not-allowed app.
        if app_id in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "skip_apps", fallback=[]
        ):
            return

        metrics_as_struct = app_id in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "metrics_as_struct", fallback=[]
        )

        # Separate apps with legacy telemetry client ID vs those that don't have it
        if app_id == "firefox_desktop":
            has_legacy_telemetry_client_id = True
        else:
            has_legacy_telemetry_client_id = False

        # Separate apps with profile group ID vs those that don't have it
        if app_id == "firefox_desktop":
            has_profile_group_id = True
        else:
            has_profile_group_id = False

        unversioned_table_name = re.sub(r"_v[0-9]+$", "", baseline_table.split(".")[-1])

        self.custom_render_kwargs = {
            "has_profile_group_id": has_profile_group_id,
            "has_legacy_telemetry_client_id": has_legacy_telemetry_client_id,
            "metrics_as_struct": metrics_as_struct,
            "has_metrics": ping_has_metrics(app_id, unversioned_table_name),
        }

        super().generate_per_app_id(
            project_id,
            baseline_table,
            output_dir,
            use_cloud_function,
            app_info,
            id_token=id_token,
        )

    def generate_per_app(
        self,
        project_id,
        app_info,
        output_dir=None,
        use_cloud_function=True,
        parallelism=8,
        id_token=None,
    ):
        """Generate the events_stream table query per app_name."""
        target_dataset = app_info[0]["app_name"]
        if target_dataset in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "skip_datasets", fallback=[]
        ):
            return

        super().generate_per_app(project_id, app_info, output_dir, id_token=id_token)
