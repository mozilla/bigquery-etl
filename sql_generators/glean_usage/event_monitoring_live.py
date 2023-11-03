"""Generate Materialized Views and aggregate queries for event monitoring."""

import os
from collections import namedtuple
from datetime import datetime
from pathlib import Path

from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from sql_generators.glean_usage.common import (
    GleanTable,
    get_app_info,
    get_table_dir,
    render,
    table_names_from_baseline,
    write_sql,
)

TARGET_TABLE_ID = "event_monitoring_live_v1"
TARGET_DATASET_CROSS_APP = "monitoring"
PREFIX = "event_monitoring"
PATH = Path(os.path.dirname(__file__))


class EventMonitoringLive(GleanTable):
    """Represents the generated materialized view for event monitoring."""

    def __init__(self) -> None:
        """Initialize materialized view generation."""
        self.no_init = False
        self.per_app_id_enabled = True
        self.per_app_enabled = False
        self.across_apps_enabled = True
        self.prefix = PREFIX
        self.target_table_id = TARGET_TABLE_ID
        self.custom_render_kwargs = {}

    def generate_per_app_id(
        self, project_id, baseline_table, output_dir=None, use_cloud_function=True
    ):
        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.init.sql"
        metadata_filename = f"{self.target_table_id}.metadata.yaml"

        table = tables[f"{self.prefix}"]
        dataset = tables[self.prefix].split(".")[-2].replace("_derived", "")

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=tables[self.prefix].split(".")[-2],
            dataset=dataset,
            current_date=datetime.today().strftime("%Y-%m-%d"),
            app_name=[
                app_dataset["canonical_app_name"]
                for _, app in get_app_info().items()
                for app_dataset in app
                if dataset == app_dataset["bq_dataset_family"]
            ][0],
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        # generated files to update
        Artifact = namedtuple("Artifact", "table_id basename sql")
        artifacts = []

        if not self.no_init:
            init_sql = render(
                init_filename, template_folder=PATH / "templates", **render_kwargs
            )
            metadata = render(
                metadata_filename,
                template_folder=PATH / "templates",
                format=False,
                **render_kwargs,
            )
            artifacts.append(Artifact(table, "metadata.yaml", metadata))

        skip_existing_artifact = self.skip_existing(output_dir, project_id)

        if output_dir:
            if not self.no_init:
                artifacts.append(Artifact(table, "init.sql", init_sql))

            for artifact in artifacts:
                destination = (
                    get_table_dir(output_dir, artifact.table_id) / artifact.basename
                )
                skip_existing = str(destination) in skip_existing_artifact

                write_sql(
                    output_dir,
                    artifact.table_id,
                    artifact.basename,
                    artifact.sql,
                    skip_existing=skip_existing,
                )

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        prod_datasets_with_baseline = [
            s.bq_dataset_family
            for s in get_stable_table_schemas()
            if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
            and s.bq_table == "baseline_v1"
        ]

        aggregate_table = "event_monitoring_aggregates_v1"
        target_view_name = "_".join(self.target_table_id.split("_")[:-1])

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{TARGET_DATASET_CROSS_APP}.{target_view_name}",
            table=target_view_name,
            target_table=f"{TARGET_DATASET_CROSS_APP}_derived.{aggregate_table}",
            apps=apps,
            prod_datasets=prod_datasets_with_baseline,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"{aggregate_table}.query.sql"
        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        metadata = render(
            f"{aggregate_table}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        table = f"{project_id}.{TARGET_DATASET_CROSS_APP}_derived.{aggregate_table}"

        view_sql = render(
            "event_monitoring_live.view.sql",
            template_folder=PATH / "templates",
            **render_kwargs,
        )
        view_metadata = render(
            "event_monitoring_live.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )

        view = f"{project_id}.{TARGET_DATASET_CROSS_APP}.{target_view_name}"
        if output_dir:
            artifacts = [
                Artifact(table, "metadata.yaml", metadata),
                Artifact(table, "query.sql", query_sql),
                Artifact(view, "metadata.yaml", view_metadata),
                Artifact(view, "view.sql", view_sql),
            ]

            for artifact in artifacts:
                destination = (
                    get_table_dir(output_dir, artifact.table_id) / artifact.basename
                )
                skip_existing = destination in skip_existing_artifacts

                write_sql(
                    output_dir,
                    artifact.table_id,
                    artifact.basename,
                    artifact.sql,
                    skip_existing=skip_existing,
                )
