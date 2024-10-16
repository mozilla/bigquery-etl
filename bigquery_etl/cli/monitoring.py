"""bigquery-etl CLI monitoring command."""

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import click
from bigeye_sdk.authentication.api_authentication import APIKeyAuth
from bigeye_sdk.bigconfig_validation.validation_context import _BIGEYE_YAML_FILE_IX
from bigeye_sdk.client.datawatch_client import datawatch_client_factory
from bigeye_sdk.client.enum import Method
from bigeye_sdk.controller.metric_suite_controller import MetricSuiteController
from bigeye_sdk.exceptions.exceptions import FileLoadException
from bigeye_sdk.model.big_config import (
    BigConfig,
    ColumnSelector,
    RowCreationTimes,
    TagDeployment,
    TagDeploymentSuite,
)
from bigeye_sdk.model.protobuf_message_facade import (
    SimpleCollection,
    SimpleConstantThreshold,
    SimpleMetricDefinition,
    SimpleMetricSchedule,
    SimpleNamedSchedule,
    SimplePredefinedMetric,
    SimplePredefinedMetricName,
)

from bigquery_etl.config import ConfigLoader
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option
from ..util import extract_from_query_path

BIGCONFIG_FILE = "bigconfig.yml"
VIEW_FILE = "view.sql"


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

                if (
                    metadata_file.parent / VIEW_FILE
                ).exists() or "public_bigquery" in metadata.labels:
                    # monitoring to be deployed on a view
                    # Bigeye requires to explicitly set the partition column for views
                    ctx.invoke(
                        set_partition_column,
                        name=metadata_file.parent,
                        sql_dir=sql_dir,
                        project_id=project_id,
                    )

        except FileNotFoundError:
            print("No metadata file for: {}.{}.{}".format(project, dataset, table))

    # Deploy BigConfig files at once.
    # Deploying BigConfig files separately can lead to previously deployed metrics being removed.
    mc.execute_bigconfig(
        input_path=[
            metadata_file.parent / BIGCONFIG_FILE
            for metadata_file in list(set(metadata_files))
        ],
        output_path=Path(sql_dir).parent if sql_dir else None,
        apply=True,
        recursive=False,
        strict_mode=True,
        auto_approve=True,
    )


def _update_bigconfig(
    bigconfig,
    metadata,
    project,
    dataset,
    table,
    default_metrics,
):
    """Update the BigConfig file to monitor a view."""
    for collection in bigconfig.tag_deployments:
        for deployment in collection.deployments:
            for metric in deployment.metrics:
                if metric.metric_type.predefined_metric in default_metrics:
                    default_metrics.remove(metric.metric_type.predefined_metric)

        if metadata.monitoring.collection and collection.collection is None:
            collection.collection = SimpleCollection(
                name=metadata.monitoring.collection
            )

    if len(default_metrics) > 0:
        deployments = [
            TagDeployment(
                column_selectors=[
                    ColumnSelector(name=f"{project}.{project}.{dataset}.{table}.*")
                ],
                metrics=[
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
            collection = SimpleCollection(name=metadata.monitoring.collection)

        bigconfig.tag_deployments += [
            TagDeploymentSuite(deployments=deployments, collection=collection)
        ]

        if metadata.monitoring.partition_column:
            bigconfig.row_creation_times = RowCreationTimes(
                column_selectors=[
                    ColumnSelector(
                        name=f"{project}.{project}.{dataset}.{table}.{metadata.monitoring.partition_column}"
                    )
                ]
            )


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

                if (
                    metadata_file.parent / VIEW_FILE
                ).exists() or "public_bigquery" in metadata.labels:
                    _update_bigconfig(
                        bigconfig=bigconfig,
                        metadata=metadata,
                        project=project,
                        dataset=dataset,
                        table=table,
                        default_metrics=[
                            SimplePredefinedMetricName.FRESHNESS_DATA,
                            SimplePredefinedMetricName.VOLUME_DATA,
                        ],
                    )
                else:
                    _update_bigconfig(
                        bigconfig=bigconfig,
                        metadata=metadata,
                        project=project,
                        dataset=dataset,
                        table=table,
                        default_metrics=[
                            SimplePredefinedMetricName.FRESHNESS,
                            SimplePredefinedMetricName.VOLUME,
                        ],
                    )

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

        if (bigconfig_file.parent / VIEW_FILE).exists():
            try:
                metadata = Metadata.from_file(bigconfig_file.parent / METADATA_FILE)
                if metadata.monitoring and metadata.monitoring.enabled:
                    if not metadata.monitoring.partition_column_set:
                        invalid = True
                        click.echo(
                            "If montoring is enabled for views, then the partition_column needs to be configured "
                            + f"explicitly in the `monitoring` metadata for {bigconfig_file.parent}. "
                            + "Set `partition_column: null` for views that do not reference a partitioned table."
                        )
            except FileNotFoundError:
                pass  # view has no metadata file, so monitoring is disabled

    if invalid:
        sys.exit(1)

    click.echo("All BigConfig files are valid.")


@monitoring.command(
    help="""
    Set partition column for view or table in Bigeye.
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
def set_partition_column(
    name: str,
    sql_dir: Optional[str],
    project_id: Optional[str],
    base_url: str,
    workspace: int,
) -> None:
    """Validate BigConfig file."""
    api_key = os.environ.get("BIGEYE_API_KEY")
    if api_key is None:
        click.echo(
            "Bigeye API token needs to be set via `BIGEYE_API_KEY` env variable."
        )
        sys.exit(1)

    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    for metadata_file in list(set(metadata_files)):
        project, dataset, table = extract_from_query_path(metadata_file)
        try:
            metadata = Metadata.from_file(metadata_file)
            if metadata.monitoring and metadata.monitoring.enabled:
                if not metadata.monitoring.partition_column_set:
                    click.echo(f"No partition column set for {metadata_file.parent}")
                    continue
                api_auth = APIKeyAuth(base_url=base_url, api_key=api_key)
                client = datawatch_client_factory(api_auth, workspace_id=workspace)

                warehouse_id = ConfigLoader.get("monitoring", "bigeye_warehouse_id")
                table_id = client.get_table_ids(
                    warehouse_id=warehouse_id,
                    schemas=f"{project}.{dataset}",
                    table_name=table,
                )

                if len(table_id) == 0:
                    click.echo(
                        f"Table `{project}.{dataset}.{table}` does not exist in Bigeye."
                    )
                    sys.exit(1)
                table_id = table_id[0]

                columns = client.get_columns(table_ids=[table_id])
                column_id = None
                for column in columns.columns:
                    if column.column.name == metadata.monitoring.partition_column:
                        column_id = column.column.id
                        break

                if column_id is None:
                    click.echo(
                        f"Configured partition column {metadata.monitoring.partition_column} does not exist for table `{project}.{dataset}.{table}`"
                    )
                    sys.exit(1)

                url = f"/api/v1/tables/{table_id}/required-partition-column"
                payload = json.dumps({"columnId": column_id})

                try:
                    response = client._call_datawatch(Method.PUT, url=url, body=payload)

                    if (
                        response.get("datasetId", None) == table_id
                        and response.get("requiredPartitionColumnId") == column_id
                    ):
                        click.echo(
                            f"Set partition column {column_id} for `{project}.{dataset}.{table}`"
                        )
                except Exception as e:
                    if "There was an error processing your request" in str(e):
                        # API throws an error when partition column was already set.
                        # There is no API endpoint to check for partition columns though
                        pass
                    else:
                        raise e

        except FileNotFoundError:
            print("No metadata file for: {}.{}.{}".format(project, dataset, table))


# TODO: remove this command once checks have been migrated
@monitoring.command(
    help="""
    Create BigConfig files from ETL check.sql files.

    This is a temporary command and will be removed after checks have been migrated.
    """
)
@click.argument("name")
@project_id_option()
@sql_dir_option
@click.pass_context
def migrate(ctx, name: str, sql_dir: Optional[str], project_id: Optional[str]) -> None:
    """Migrate checks.sql files to BigConfig."""
    check_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["checks.sql"]
    )

    for check_file in list(set(check_files)):
        project, dataset, table = extract_from_query_path(check_file)
        checks_migrated = 0
        try:
            metadata_file = check_file.parent / METADATA_FILE
            metadata = Metadata.from_file(check_file.parent / METADATA_FILE)
            metadata.monitoring = {"enabled": True}
            metadata.write(metadata_file)

            ctx.invoke(
                update,
                name=metadata_file.parent,
                sql_dir=sql_dir,
                project_id=project_id,
            )

            _BIGEYE_YAML_FILE_IX.clear()
            bigconfig_file = check_file.parent / BIGCONFIG_FILE
            if bigconfig_file.exists():
                bigconfig = BigConfig.load(bigconfig_file)
            else:
                bigconfig = BigConfig(type="BIGCONFIG_FILE")

            checks = check_file.read_text()
            metrics_by_column = defaultdict(list)

            # map pre-defined ETL checks to Bigeye checks
            if matches := re.findall(
                r"not_null\(\[([^\]\)]*)\](?:, \"(.*)\")?\)", checks
            ):
                for match in matches:
                    checks_migrated += 1
                    columns = [
                        col.replace('"', "")
                        for col in match[0]
                        .replace("[", "")
                        .replace("]", "")
                        .split(", ")
                    ]
                    metric_definition = SimpleMetricDefinition(
                        metric_type=SimplePredefinedMetric(
                            type="PREDEFINED",
                            predefined_metric=SimplePredefinedMetricName.PERCENT_NOT_NULL,
                        ),
                        threshold=SimpleConstantThreshold(
                            type="CONSTANT", lower_bound=1.0
                        ),
                    )

                    for column in columns:
                        metrics_by_column[
                            f"{project}.{project}.{dataset}.{table}.{column}"
                        ].append(metric_definition)

            if matches := re.findall(
                r"min_row_count\(([^,\)]*)(?:, \"(.*)\")?\)", checks
            ):
                for match in matches:
                    checks_migrated += 1
                    metrics_by_column[
                        f"{project}.{project}.{dataset}.{table}.*"
                    ].append(
                        SimpleMetricDefinition(
                            metric_type=SimplePredefinedMetric(
                                type="PREDEFINED",
                                predefined_metric=SimplePredefinedMetricName.COUNT_ROWS,
                            ),
                            threshold=SimpleConstantThreshold(
                                type="CONSTANT", lower_bound=int(match[0])
                            ),
                        )
                    )

            if matches := re.findall(r"is_unique\(([^\]\)]*)(?:, \"(.*)\")?\)", checks):
                for match in matches:
                    checks_migrated += 1
                    columns = [
                        col.replace('"', "")
                        for col in match[0]
                        .replace("[", "")
                        .replace("]", "")
                        .split(", ")
                    ]
                    metric_definition = SimpleMetricDefinition(
                        metric_type=SimplePredefinedMetric(
                            type="PREDEFINED",
                            predefined_metric=SimplePredefinedMetricName.COUNT_DUPLICATES,
                        ),
                        threshold=SimpleConstantThreshold(
                            type="CONSTANT", lower_bound=0.0
                        ),
                    )

                    for column in columns:
                        metrics_by_column[
                            f"{project}.{project}.{dataset}.{table}.{column}"
                        ].append(metric_definition)

            if matches := re.findall(r"in_range\((.*), (.*), (.*), \"(.*)\"\)", checks):
                for match in matches:
                    checks_migrated += 1
                    columns = [
                        col.replace('"', "")
                        for col in match[0]
                        .replace("[", "")
                        .replace("]", "")
                        .split(", ")
                    ]
                    metric_definition = SimpleMetricDefinition(
                        metric_name="Range",
                        metric_type=SimplePredefinedMetric(
                            type="PREDEFINED",
                            predefined_metric=SimplePredefinedMetricName.MIN,
                        ),
                        threshold=SimpleConstantThreshold(
                            type="CONSTANT",
                            lower_bound=int(match[1]) if match[1] != "none" else None,
                            upper_bound=int(match[2]) if match[2] != "none" else None,
                        ),
                    )

                    for column in columns:
                        metrics_by_column[
                            f"{project}.{project}.{dataset}.{table}.{column}"
                        ].append(metric_definition)

            if matches := re.findall(
                r"value_length\(column=\"(.*)\", expected_length=(.*), where=\"(.*)\"\)",
                checks,
            ):
                for match in matches:
                    checks_migrated += 1
                    metric_definition = SimpleMetricDefinition(
                        metric_name="Value Length",
                        metric_type=SimplePredefinedMetric(
                            type="PREDEFINED",
                            predefined_metric=SimplePredefinedMetricName.STRING_LENGTH_MIN,
                        ),
                        threshold=SimpleConstantThreshold(
                            type="CONSTANT",
                            lower_bound=int(match[1]),
                            upper_bound=int(match[1]),
                        ),
                    )

                    for column in columns:
                        metrics_by_column[
                            f"{project}.{project}.{dataset}.{table}.{column}"
                        ].append(metric_definition)

            deployments = []
            for column_selector, metrics in metrics_by_column.items():
                deployments.append(
                    TagDeployment(
                        column_selectors=[ColumnSelector(name=column_selector)],
                        metrics=metrics,
                    )
                )

            bigconfig.tag_deployments += [TagDeploymentSuite(deployments=deployments)]

            bigconfig.save(
                output_path=bigconfig_file.parent,
                default_file_name=bigconfig_file.stem,
            )

            total_checks = checks.count("#fail") + checks.count("#warn")
            click.echo(
                f"Migrated {checks_migrated} of {total_checks} checks to {bigconfig_file}."
            )
            if checks_migrated < total_checks:
                click.echo(
                    f"There might be custom SQL checks that need to be migrated manually for {check_file.parent}"
                )

        except FileNotFoundError:
            print(f"No metadata file for: {check_file.parent}")

    click.echo(
        "Please manually check the migration logic as it might not be 100% correct"
    )
