"""bigquery-etl CLI monitoring command."""

import os
import sys
from pathlib import Path
from typing import Optional

import click
from bigeye_sdk.authentication.api_authentication import APIKeyAuth
from bigeye_sdk.client.datawatch_client import datawatch_client_factory
from bigeye_sdk.controller.metric_suite_controller import MetricSuiteController
from bigeye_sdk.exceptions.exceptions import FileLoadException
from bigeye_sdk.model.big_config import BigConfig, TableDeployment, TableDeploymentSuite
from bigeye_sdk.model.protobuf_message_facade import (
    SimpleCollection,
    SimpleMetricDefinition,
    SimpleMetricSchedule,
    SimpleNamedSchedule,
    SimplePredefinedMetric,
    SimplePredefinedMetricName,
)

from bigquery_etl.metadata.parse_metadata import Metadata

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option
from ..util import extract_from_query_path

BIGCONFIG_FILE = "bigconfig.yml"


@click.group(
    help="""
        Commands for managing monitoring of datasets.
        """
)
@click.pass_context
def monitoring(ctx):
    """Create the CLI group for the monitoring command."""
    pass


@monitoring.command(
    help="""
    Deploy monitors defined in the BigConfig files to Bigeye.

    Requires BigConfig API key to be set via BIGEYE_API_KEY env variable.
    """
)
@click.argument("name")
@project_id_option()
@sql_dir_option
@click.option(
    "--workspace",
    default=463,
    help="Bigeye workspace to use when authenticating to API.",
)
@click.option(
    "--base-url",
    "--base_url",
    default="https://app.bigeye.com",
    help="Bigeye base URL.",
)
@click.pass_context
def deploy(
    ctx,
    name: str,
    sql_dir: Optional[str],
    project_id: Optional[str],
    workspace: str,
    base_url: str,
) -> None:
    """Deploy Bigeye config."""
    api_key = os.environ.get("BIGEYE_API_KEY")
    if api_key is None:
        click.echo(
            "Bigeye API token needs to be set via `BIGEYE_API_KEY` env variable."
        )
        sys.exit(1)

    """Deploy monitors to Bigeye."""
    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    for metadata_file in list(set(metadata_files)):
        project, dataset, table = extract_from_query_path(metadata_file)
        try:
            metadata = Metadata.from_file(metadata_file)
            if metadata.monitoring and metadata.monitoring.enabled:
                ctx.invoke(
                    update,
                    name=metadata_file.parent,
                    sql_dir=sql_dir,
                    project_id=project_id,
                )
                api_auth = APIKeyAuth(base_url=base_url, api_key=api_key)
                client = datawatch_client_factory(api_auth, workspace_id=workspace)
                mc = MetricSuiteController(client=client)

                ctx.invoke(
                    validate,
                    name=metadata_file.parent,
                    sql_dir=sql_dir,
                    project_id=project_id,
                )
                mc.execute_bigconfig(
                    input_path=[metadata_file.parent / BIGCONFIG_FILE],
                    output_path=Path(sql_dir).parent if sql_dir else None,
                    apply=True,
                    recursive=False,
                    strict_mode=True,
                    auto_approve=True,
                )

        except FileNotFoundError:
            print("No metadata file for: {}.{}.{}".format(project, dataset, table))


@monitoring.command(
    help="""
    Update BigConfig files based on monitoring metadata.

    Requires BigConfig credentials to be set via BIGEYE_API_CRED_FILE env variable.
    """
)
@click.argument("name")
@project_id_option()
@sql_dir_option
def update(name: str, sql_dir: Optional[str], project_id: Optional[str]) -> None:
    """Update BigConfig files based on monitoring metadata."""
    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    for metadata_file in list(set(metadata_files)):
        project, dataset, table = extract_from_query_path(metadata_file)
        try:
            metadata = Metadata.from_file(metadata_file)
            if metadata.monitoring and metadata.monitoring.enabled:
                bigconfig_file = metadata_file.parent / BIGCONFIG_FILE

                if bigconfig_file.exists():
                    bigconfig = BigConfig.load(bigconfig_file)
                else:
                    bigconfig = BigConfig(type="BIGCONFIG_FILE")

                default_metrics = [
                    SimplePredefinedMetricName.FRESHNESS,
                    SimplePredefinedMetricName.VOLUME,
                ]

                for collection in bigconfig.table_deployments:
                    for deployment in collection.deployments:
                        for metric in deployment.table_metrics:
                            if metric.metric_type.predefined_metric in default_metrics:
                                default_metrics.remove(
                                    metric.metric_type.predefined_metric
                                )

                    if metadata.monitoring.collection and collection.collection is None:
                        collection.collection = SimpleCollection(
                            name=metadata.monitoring.collection
                        )

                if len(default_metrics) > 0:
                    deployments = [
                        TableDeployment(
                            fq_table_name=f"{project}.{project}.{dataset}.{table}",
                            table_metrics=[
                                SimpleMetricDefinition(
                                    metric_type=SimplePredefinedMetric(
                                        type="PREDEFINED", predefined_metric=metric
                                    ),
                                    metric_schedule=SimpleMetricSchedule(
                                        named_schedule=SimpleNamedSchedule(
                                            name="Default Schedule - 13:00 UTC"
                                        )
                                    ),
                                )
                                for metric in default_metrics
                            ],
                        )
                    ]

                    collection = None
                    if metadata.monitoring.collection:
                        collection = SimpleCollection(
                            name=metadata.monitoring.collection
                        )

                    bigconfig.table_deployments += [
                        TableDeploymentSuite(
                            deployments=deployments, collection=collection
                        )
                    ]

                bigconfig.save(
                    output_path=bigconfig_file.parent,
                    default_file_name=bigconfig_file.stem,
                )

        except FileNotFoundError:
            print("No metadata file for: {}.{}.{}".format(project, dataset, table))


@monitoring.command(
    help="""
    Validate BigConfig files.
    """
)
@click.argument("name")
@project_id_option()
@sql_dir_option
def validate(name: str, sql_dir: Optional[str], project_id: Optional[str]) -> None:
    """Validate BigConfig file."""
    bigconfig_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["bigconfig.yml"]
    )

    invalid = False

    for bigconfig_file in list(set(bigconfig_files)):
        try:
            BigConfig.load(bigconfig_file)
        except FileLoadException as e:
            if "Duplicate" in e.message:
                pass
            else:
                click.echo(f"Invalid BigConfig file {bigconfig_file}: {e}")
                invalid = True
        except Exception as e:
            click.echo(f"Invalid BigConfig file {bigconfig_file}: {e}")
            invalid = True

    if invalid:
        sys.exit(1)

    click.echo("All BigConfig files are valid.")
