"""Generate Materialized Views for event monitoring."""

from sql_generators.glean_usage.common import (
    GleanTable,
    table_names_from_baseline,
    render,
    get_table_dir,
    write_sql,
    write_dataset_metadata,
)
from pathlib import Path
import os
from jinja2 import TemplateNotFound
from collections import namedtuple


TARGET_TABLE_ID = "event_monitoring_live_v1"
TARGET_DATASET_CROSS_APP = "monitoring_derived"
PREFIX = "event_monitoring_live"
PATH = Path(os.path.dirname(__file__))


class EventMonitoringMaterializedView(GleanTable):
    """Represents the generated materialized view for event monitoring."""

    def __init__(self) -> None:
        """Initialize materialized view generation."""
        self.no_init = False
        self.per_app_id_enabled = True
        self.per_app_enabled = True
        self.prefix = PREFIX
        self.target_table_id = TARGET_TABLE_ID

    def generate_per_app_id(
        self, project_id, baseline_table, output_dir=None, use_cloud_function=True
    ):
        tables = table_names_from_baseline(baseline_table, include_project_id=False)

        init_filename = f"{self.target_table_id}.init.sql"
        metadata_filename = f"{self.target_table_id}.metadata.yaml"

        table = tables[f"{self.prefix}_table"]

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            derived_dataset=tables["event_monitoring_live"].split(".")[-2],
        )

        render_kwargs.update(self.custom_render_kwargs)
        render_kwargs.update(tables)

        # generated files to update
        Artifact = namedtuple("Artifact", "table_id basename sql")
        artifacts = []

        if not self.no_init:
            try:
                init_sql = render(
                    init_filename, template_folder=PATH / "templates", **render_kwargs
                )
            except TemplateNotFound:
                init_sql = render(
                    init_filename,
                    template_folder=PATH / "templates",
                    init=True,
                    **render_kwargs,
                )
                artifacts.append(Artifact(table, "init.sql", init_sql))

                metadata = render(
                    metadata_filename,
                    template_folder=PATH / "templates",
                    format=False,
                    **render_kwargs,
                )
                artifacts.append(Artifact(table, "metadata.yaml", metadata))

        skip_existing_artifact = self.skip_existing(output_dir, project_id)

        if output_dir:
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

    def generate_per_app(
        self, project_id, app_info, output_dir=None, use_cloud_function=True
    ):
        """Generate the baseline table query per app_name."""
        if not self.per_app_enabled:
            return

        target_view_name = "_".join(self.target_table_id.split("_")[:-1])
        target_dataset = app_info[0]["app_name"]

        datasets = [
            (a["bq_dataset_family"], a.get("app_channel", "release")) for a in app_info
        ]

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{target_dataset}.{target_view_name}",
            datasets=datasets,
            table=target_view_name,
            target_table=f"{target_dataset}_derived.{self.target_table_id}",
            app_name=app_info[0]["app_name"],
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        query_filename = f"event_monitoring_aggregates_v1.query.sql"
        query_sql = render(
            query_filename, template_folder=PATH / "templates", **render_kwargs
        )
        view_sql = render(
            f"{target_view_name}.view.sql",
            template_folder=PATH / "templates",
            **render_kwargs,
        )
        metadata = render(
            f"{self.target_table_id[:-3]}.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        table = f"{project_id}.{target_dataset}_derived.event_monitoring_aggregates_v1"
        view = f"{project_id}.{target_dataset}.event_monitoring_live"
        if output_dir:
            artifacts = [
                Artifact(table, "query.sql", query_sql),
                Artifact(table, "metadata.yaml", metadata),
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

            write_dataset_metadata(output_dir, view)
            write_dataset_metadata(output_dir, table, derived_dataset_metadata=True)

    def generate_across_apps(
        self, project_id, apps, output_dir=None, use_cloud_function=True
    ):
        """Generate a query across all apps."""
        if not self.across_apps_enabled:
            return

        target_view_name = "_".join(self.target_table_id.split("_")[:-1])

        render_kwargs = dict(
            header="-- Generated via bigquery_etl.glean_usage\n",
            header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
            project_id=project_id,
            target_view=f"{TARGET_DATASET_CROSS_APP}.{target_view_name}",
            table=target_view_name,
            target_table=f"{TARGET_DATASET_CROSS_APP}.{self.target_table_id}",
            apps=apps,
        )
        render_kwargs.update(self.custom_render_kwargs)

        skip_existing_artifacts = self.skip_existing(output_dir, project_id)

        Artifact = namedtuple("Artifact", "table_id basename sql")

        view_sql = render(
            f"event_monitoring_live_cross_app.view.sql",
            template_folder=PATH / "templates",
            **render_kwargs,
        )
        metadata = render(
            f"event_monitoring_live_cross_app.metadata.yaml",
            template_folder=PATH / "templates",
            format=False,
            **render_kwargs,
        )
        view = f"{project_id}.{TARGET_DATASET_CROSS_APP}.event_monitoring_live_v1"
        if output_dir:
            artifacts = [
                Artifact(view, "metadata.yaml", metadata),
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
