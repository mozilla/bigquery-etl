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
        self.common_render_kwargs = {}
        self.possible_query_parameters = {
            "submission_date": "DATE",
            "min_sample_id": "INTEGER",
            "max_sample_id": "INTEGER",
        }

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
        """Generate the events_stream table query per app_id."""
        # Skip any not-allowed app.
        if app_name in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "skip_apps", fallback=[]
        ):
            return

        metrics_as_struct = app_name in ConfigLoader.get(
            "generate",
            "glean_usage",
            "events_stream",
            "metrics_as_struct_apps",
            fallback=[],
        )

        slice_by_sample_id = app_name in ConfigLoader.get(
            "generate",
            "glean_usage",
            "events_stream",
            "slice_by_sample_id_apps",
            fallback=[],
        )

        # Separate apps with legacy telemetry client ID vs those that don't have it
        if app_name == "firefox_desktop":
            has_legacy_telemetry_client_id = True
        else:
            has_legacy_telemetry_client_id = False

        # Separate apps with profile group ID vs those that don't have it
        if app_name == "firefox_desktop":
            has_profile_group_id = True
        else:
            has_profile_group_id = False

        unversioned_table_name = re.sub(r"_v[0-9]+$", "", baseline_table.split(".")[-1])

        custom_render_kwargs = {
            "has_profile_group_id": has_profile_group_id,
            "has_legacy_telemetry_client_id": has_legacy_telemetry_client_id,
            "metrics_as_struct": metrics_as_struct,
            "has_metrics": ping_has_metrics(
                app_id_info["bq_dataset_family"], unversioned_table_name
            ),
            "slice_by_sample_id": slice_by_sample_id,
        }

        super().generate_per_app_id(
            project_id,
            baseline_table,
            app_name,
            app_id_info,
            output_dir=output_dir,
            use_cloud_function=use_cloud_function,
            parallelism=parallelism,
            id_token=id_token,
            custom_render_kwargs=custom_render_kwargs,
            use_python_query=slice_by_sample_id,
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
        """Generate the events_stream table query per app_name."""
        if app_name in ConfigLoader.get(
            "generate", "glean_usage", "events_stream", "skip_apps", fallback=[]
        ):
            return

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
