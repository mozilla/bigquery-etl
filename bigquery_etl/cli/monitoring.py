"""bigquery-etl CLI monitoring command."""

import ast
import json
import os
import sys
from pathlib import Path
from typing import Optional

import click
import sqlglot
from bigeye_sdk.authentication.api_authentication import APIKeyAuth
from bigeye_sdk.client.datawatch_client import datawatch_client_factory
from bigeye_sdk.client.enum import Method
from bigeye_sdk.controller.metric_suite_controller import MetricSuiteController
from bigeye_sdk.exceptions.exceptions import FileLoadException
from bigeye_sdk.model.big_config import (
    BigConfig,
    ColumnSelector,
    RowCreationTimes,
    TableDeployment,
    TableDeploymentSuite,
    TagDeployment,
    TagDeploymentSuite,
)
from bigeye_sdk.model.protobuf_message_facade import (
    SimpleCollection,
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
from ..util.common import render as render_template

BIGCONFIG_FILE = "bigconfig.yml"
CUSTOM_RULES_FILE = "bigconfig_custom_rules.sql"
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

                if (metadata_file.parent / VIEW_FILE).exists():
                    # monitoring to be deployed on a view
                    # Bigeye requires to explicitly set the partition column for views
                    ctx.invoke(
                        set_partition_column,
                        name=metadata_file.parent,
                        sql_dir=sql_dir,
                        project_id=project_id,
                    )

                if (metadata_file.parent / CUSTOM_RULES_FILE).exists():
                    ctx.invoke(
                        deploy_custom_rules,
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


@monitoring.command(
    help="""
    Deploy custom SQL rules.
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
def deploy_custom_rules(
    name: str,
    sql_dir: Optional[str],
    project_id: Optional[str],
    base_url: str,
    workspace: int,
) -> None:
    """Deploy custom SQL rules for files."""
    api_key = os.environ.get("BIGEYE_API_KEY")
    if api_key is None:
        click.echo(
            "Bigeye API token needs to be set via `BIGEYE_API_KEY` env variable."
        )
        sys.exit(1)

    custom_rules_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=[CUSTOM_RULES_FILE]
    )

    api_auth = APIKeyAuth(base_url=base_url, api_key=api_key)
    client = datawatch_client_factory(api_auth, workspace_id=workspace)
    collections = client.get_collections()
    warehouse_id = ConfigLoader.get("monitoring", "bigeye_warehouse_id")
    existing_rules = client.get_rules_for_source(warehouse_id=warehouse_id)
    existing_rules_sql = [rule.custom_rule.sql for rule in existing_rules.custom_rules]
    existing_schedules = {
        schedule.name: schedule.id
        for schedule in client.get_named_schedule().named_schedules
    }
    url = "/api/v1/custom-rules"

    for custom_rule_file in list(set(custom_rules_files)):
        project, dataset, table = extract_from_query_path(custom_rule_file)
        try:
            metadata = Metadata.from_file(custom_rule_file.parent / METADATA_FILE)
            if metadata.monitoring and metadata.monitoring.enabled:
                # Convert all the Airflow params to jinja usable dict.
                jinja_params = {
                    **{
                        "dataset_id": dataset,
                        "table_name": table,
                        "project_id": project,
                    },
                }
                if "format" not in jinja_params:
                    jinja_params["format"] = False

                rendered_result = render_template(
                    custom_rule_file.name,
                    template_folder=str(custom_rule_file.parent),
                    templates_dir="",
                    **jinja_params,
                )

                for statement in sqlglot.parse(rendered_result, read="bigquery"):
                    if statement is None:
                        continue

                    for select_statement in statement.find_all(sqlglot.exp.Select):
                        sql = select_statement.sql(dialect="bigquery")
                        if sql in existing_rules_sql:
                            continue

                        config = {}
                        if select_statement.comments:
                            config = ast.literal_eval(
                                "".join(select_statement.comments)
                            )

                        payload = {
                            "name": config.get(
                                "name", f"{project}_{dataset}_{table}_bqetl_check"
                            ),
                            "sql": sql,
                            "warehouseId": warehouse_id,
                            "thresholdType": "CUSTOM_RULES_THRESHOLD_TYPE_"
                            + config.get("alert_type", "count").upper(),
                        }

                        if "range" in config:
                            if "min" in config["range"]:
                                payload["lowerThreshold"] = config["range"]["min"]
                            if "max" in config["range"]:
                                payload["upperThreshold"] = config["range"]["max"]

                        if "owner" in config:
                            payload["owner"] = {"email": config["owner"]}

                        if "collections" in config:
                            collection_ids = [
                                collection.id
                                for collection in collections.collections
                                if collection.name in config["collections"]
                            ]
                            payload["collectionIds"] = collection_ids

                        if (
                            "schedule" in config
                            and config["schedule"] in existing_schedules
                        ):
                            payload["metricSchedule"] = {
                                "namedSchedule": {
                                    "id": existing_schedules[config["schedule"]]
                                }
                            }

                        try:
                            response = client._call_datawatch(
                                Method.POST,
                                url=url,
                                body=json.dumps({"customRule": payload}),
                            )

                            if "id" in response:
                                click.echo(
                                    f"Created custom rule {response['id']} for `{project}.{dataset}.{table}`"
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


def _update_table_bigconfig(
    bigconfig,
    metadata,
    project,
    dataset,
    table,
):
    """Update the BigConfig file to monitor a table."""
    default_metrics = [
        SimplePredefinedMetricName.FRESHNESS,
        SimplePredefinedMetricName.VOLUME,
    ]

    for collection in bigconfig.table_deployments:
        for deployment in collection.deployments:
            for metric in deployment.table_metrics:
                if metric.metric_type.predefined_metric in default_metrics:
                    default_metrics.remove(metric.metric_type.predefined_metric)

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
            collection = SimpleCollection(name=metadata.monitoring.collection)

        bigconfig.table_deployments += [
            TableDeploymentSuite(deployments=deployments, collection=collection)
        ]


def _update_view_bigconfig(
    bigconfig,
    metadata,
    project,
    dataset,
    table,
):
    """Update the BigConfig file to monitor a view."""
    default_metrics = [
        SimplePredefinedMetricName.FRESHNESS_DATA,
        SimplePredefinedMetricName.VOLUME_DATA,
    ]

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

                if (metadata_file.parent / VIEW_FILE).exists():
                    _update_view_bigconfig(
                        bigconfig=bigconfig,
                        metadata=metadata,
                        project=project,
                        dataset=dataset,
                        table=table,
                    )
                else:
                    _update_table_bigconfig(
                        bigconfig=bigconfig,
                        metadata=metadata,
                        project=project,
                        dataset=dataset,
                        table=table,
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
