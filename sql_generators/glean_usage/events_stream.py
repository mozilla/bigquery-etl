"""Generate events stream queries for Glean apps."""

import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import cache
from typing import Optional

from bigquery_etl.config import ConfigLoader
from sql_generators.glean_usage.common import (
    GleanTable,
    get_glean_app_metrics,
    get_glean_repositories,
    ping_has_metrics,
)

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
            "extras_by_type": get_glean_app_event_extras_by_type(
                app_id_info["v1_name"]
            ),
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

        extras_by_type = defaultdict(set)
        for app_id_info in app_ids_info:
            app_id_extras_by_type = get_glean_app_event_extras_by_type(
                app_id_info["v1_name"]
            )
            for app_id_extra_type, app_id_extras in app_id_extras_by_type.items():
                extras_by_type[app_id_extra_type].update(app_id_extras)

        custom_render_kwargs = {"extras_by_type": extras_by_type}

        super().generate_per_app(
            project_id,
            app_name,
            app_ids_info,
            output_dir=output_dir,
            use_cloud_function=use_cloud_function,
            parallelism=parallelism,
            id_token=id_token,
            all_base_tables_exist=all_base_tables_exist,
            custom_render_kwargs=custom_render_kwargs,
        )


@cache
def get_glean_app_repository(v1_name: str) -> dict:
    """Return the Glean app's repository."""
    for repository in get_glean_repositories():
        if repository["name"] == v1_name:
            return repository
    raise Exception(f"No Glean repository found for app `{v1_name}`.")


@cache
def get_glean_dependency_v1_name(dependency: str) -> str:
    """Return the v1 name of the Glean dependency, which may be a library."""
    for repository in get_glean_repositories():
        if "library_names" in repository and dependency in repository["library_names"]:
            return repository["name"]
    return dependency


@cache
def get_glean_app_event_extras_by_type(
    v1_name: str, ping: str = "events", cutoff_date: Optional[date] = None
) -> dict[str, set[str]]:
    """Return the Glean app's event extra keys for the specified ping, grouped by type."""
    extras_by_type = defaultdict(set)
    repository = get_glean_app_repository(v1_name)

    if not cutoff_date:
        ping_delete_after_days = (
            (
                repository.get("moz_pipeline_metadata", {})
                .get(ping, {})
                .get("expiration_policy", {})
                .get("delete_after_days")
            )
            or (
                repository.get("moz_pipeline_metadata_defaults", {})
                .get("expiration_policy", {})
                .get("delete_after_days")
            )
            or 775
        )
        cutoff_date = date.today() - timedelta(days=(ping_delete_after_days - 1))

    metrics = list(get_glean_app_metrics(v1_name).values())
    for metric in metrics:
        if metric["type"] == "event":
            for history in metric["history"]:
                last_date = datetime.fromisoformat(history["dates"]["last"]).date()
                if ping in history["send_in_pings"] and last_date >= cutoff_date:
                    for extra_key, extra_key_info in history["extra_keys"].items():
                        extra_type = extra_key_info.get("type", "string")
                        extras_by_type[extra_type].add(extra_key)

    for dependency in repository["dependencies"]:
        dependency_v1_name = get_glean_dependency_v1_name(dependency)
        dependency_extras_by_type = get_glean_app_event_extras_by_type(
            dependency_v1_name, ping, cutoff_date
        )
        for (
            depencency_extra_type,
            depencency_extras,
        ) in dependency_extras_by_type.items():
            extras_by_type[depencency_extra_type].update(depencency_extras)

    return extras_by_type
