"""bigquery-etl CLI metadata command."""

import csv
import io
import json
import re
from datetime import datetime
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Optional

import click
import requests
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from bigquery_etl.datahub.auth import LoginError
from bigquery_etl.datahub.lineage import (
    DATAHUB_LINEAGE_DEGREES,
    DEFAULT_LOOKER_TMP_DAYS,
    AuthError,
    DataHubClient,
    LineageTooLargeError,
    format_text,
)
from bigquery_etl.datahub.lineage import (
    get_downstream_lineage as get_downstream_lineage_from_datahub,
)
from bigquery_etl.metadata.parse_metadata import (
    DatasetMetadata,
    Metadata,
    WorkgroupAccessMetadata,
)
from bigquery_etl.metadata.publish_metadata import publish_metadata
from bigquery_etl.metadata.validate_metadata import (
    MetadataValidationError,
    validate_default_table_workgroup_access,
)
from bigquery_etl.metadata.validate_metadata import (
    validate_deprecation as validate_deprecation_metadata,
)
from bigquery_etl.metadata.validate_metadata import validate_workgroup_access

from ..cli.utils import (
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    sql_dir_option,
)
from ..config import ConfigLoader
from ..dryrun import get_credentials
from ..util import extract_from_query_path
from ..util.common import block_coding_agents


@click.group(help="""
    Commands for managing bqetl metadata.
    """)
@click.pass_context
def metadata(ctx):
    """Create the CLI group for the metadata command."""
    pass


@metadata.command(
    help="""
    Update metadata yaml files.
    Updates workgroup access metadata based on the dataset_metadata.yaml and
    deprecation metadata.

    Example:
     ./bqetl metadata update ga_derived.downloads_with_attribution_v2
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@project_id_option()
@sql_dir_option
@parallelism_option()
def update(
    name: str, sql_dir: Optional[str], project_id: Optional[str], parallelism: int
) -> None:
    """Update metadata yaml file."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    retained_dataset_roles = ConfigLoader.get(
        "deprecation", "retain_dataset_roles", fallback=[]
    )

    # group table metadata files by dataset
    datasets: dict[Path, list[Path]] = {}
    for table_metadata_file in table_metadata_files:
        dataset_path = Path(table_metadata_file).parent.parent
        if dataset_path not in datasets:
            datasets[dataset_path] = []
        datasets[dataset_path].append(table_metadata_file)

    # process each dataset in parallel
    if parallelism > 1:
        with Pool(parallelism) as pool:
            pool.map(
                partial(_update_dataset_metadata, retained_dataset_roles),
                datasets.items(),
            )
    else:
        for dataset_path, dataset_table_files in datasets.items():
            _update_dataset_metadata(
                retained_dataset_roles, (dataset_path, dataset_table_files)
            )

    return None


def _update_dataset_metadata(retained_dataset_roles, dataset_info):
    """Update all metadata files for a single dataset."""
    dataset_path, table_metadata_files = dataset_info

    # process all table metadata files in this dataset
    for table_metadata_file in table_metadata_files:
        try:
            dataset_metadata_path = (
                Path(table_metadata_file).parent.parent / "dataset_metadata.yaml"
            )
            if not dataset_metadata_path.exists():
                continue

            dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
            table_metadata = Metadata.from_file(table_metadata_file)

            dataset_metadata_updated = False
            table_metadata_updated = False

            # set dataset metadata default_table_workgroup_access to table_workgroup_access if not set
            if not dataset_metadata.default_table_workgroup_access:
                dataset_metadata.default_table_workgroup_access = (
                    dataset_metadata.workgroup_access
                )
                dataset_metadata_updated = True

            if table_metadata.deprecated:
                # filter table workgroup_access to only retained roles
                if table_metadata.workgroup_access is not None:
                    table_metadata.workgroup_access = [
                        workgroup
                        for workgroup in table_metadata.workgroup_access
                        if workgroup.role in retained_dataset_roles
                    ]
                else:
                    table_metadata.workgroup_access = []
                table_metadata_updated = True

                # filter dataset workgroup_access to only retained roles
                dataset_metadata.workgroup_access = [
                    workgroup
                    for workgroup in dataset_metadata.workgroup_access
                    if workgroup.get("role") in retained_dataset_roles
                ]
                dataset_metadata_updated = True

                # if dataViewer role exists in default_table_workgroup_access, ensure metadataViewer is present in workgroup_access
                # https://mozilla-hub.atlassian.net/browse/DENG-8843
                data_viewer = next(
                    (
                        workgroup
                        for workgroup in dataset_metadata.default_table_workgroup_access
                        if workgroup.get("role") == "roles/bigquery.dataViewer"
                    ),
                    None,
                )
                if data_viewer:
                    has_metadata_viewer = any(
                        workgroup.get("role") == "roles/bigquery.metadataViewer"
                        for workgroup in dataset_metadata.workgroup_access
                    )
                    if not has_metadata_viewer:
                        dataset_metadata.workgroup_access.append(
                            {
                                "role": "roles/bigquery.metadataViewer",
                                "members": sorted(data_viewer.get("members", [])),
                            }
                        )
            else:
                if table_metadata.workgroup_access is None:
                    table_metadata.workgroup_access = []

                for (
                    default_workgroup_access
                ) in dataset_metadata.default_table_workgroup_access:
                    role_exists = False
                    for i, table_workgroup_access in enumerate(
                        table_metadata.workgroup_access
                    ):
                        if table_workgroup_access.role == default_workgroup_access.get(
                            "role"
                        ):
                            role_exists = True
                            table_metadata.workgroup_access[i].members = sorted(
                                set(table_workgroup_access.members)
                                | set(default_workgroup_access.get("members", []))
                            )
                            table_metadata_updated = True

                    if not role_exists:
                        table_metadata.workgroup_access.append(
                            WorkgroupAccessMetadata(
                                role=default_workgroup_access["role"],
                                members=sorted(
                                    default_workgroup_access.get("members", [])
                                ),
                            )
                        )
                        table_metadata_updated = True

            if dataset_metadata_updated:
                dataset_metadata.write(dataset_metadata_path)
                click.echo(f"Updated {dataset_metadata_path}")
            if table_metadata_updated:
                table_metadata.write(table_metadata_file)
                click.echo(f"Updated {table_metadata_file}")
        except Exception as e:
            click.echo(f"Error processing {table_metadata_file}: {e}", err=True)
            raise e


@metadata.command(
    help="""
    Publish all metadata based on metadata.yaml file.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.

    Example:
     ./bqetl metadata publish ga_derived.downloads_with_attribution_v2
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@block_coding_agents
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
@parallelism_option()
@click.option(
    "--skip-stable-datasets",
    default=True,
    type=bool,
    help="Skip metadata publishing for tables in *_stable datasets which are managed by infra deploys.",
)
def publish(
    name: str,
    sql_dir: Optional[str],
    project_id: Optional[str],
    parallelism: int,
    skip_stable_datasets: bool,
) -> None:
    """Publish Bigquery metadata."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    # https://mozilla-hub.atlassian.net/browse/SVCSE-4108
    if skip_stable_datasets:
        table_metadata_files = [
            metadata_file
            for metadata_file in table_metadata_files
            if re.fullmatch(
                r".+/moz-fx-data-shared-prod/[a-z0-9_]+_stable/[a-z0-9_]+/metadata.yaml$",
                str(metadata_file),
                flags=re.IGNORECASE,
            )
            is None
        ]

    if parallelism > 1:
        credentials = get_credentials()

        with Pool(parallelism) as pool:
            pool.map(
                partial(_publish_metadata, project_id, credentials),
                table_metadata_files,
            )
    else:
        for metadata_file in table_metadata_files:
            _publish_metadata(project_id, credentials=None, metadata_file=metadata_file)


def _publish_metadata(project_id, credentials, metadata_file):
    project, dataset, table = extract_from_query_path(metadata_file)
    try:
        metadata = Metadata.from_file(metadata_file)
        publish_metadata(
            bigquery.Client(project=project_id, credentials=credentials),
            project,
            dataset,
            table,
            metadata,
        )
    except FileNotFoundError:
        print("No metadata file for: {}.{}.{}".format(project, dataset, table))


@metadata.command(help="""
    Deprecate BigQuery table by updating metadata.yaml file.
    Deletion date is by default 3 months from current date if not provided.

    Example:
     ./bqetl metadata deprecate ga_derived.downloads_with_attribution_v2 --deletion_date=2024-03-02
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
@click.option(
    "--deletion_date",
    "--deletion-date",
    help="Date when table is scheduled for deletion. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.today() + relativedelta(months=+3),
)
def deprecate(
    name: str,
    sql_dir: str,
    project_id: str,
    deletion_date: datetime,
) -> None:
    """Deprecate Bigquery table by updating metadata yaml file(s)."""
    table_metadata_files = list(
        set(
            paths_matching_name_pattern(
                name, sql_dir, project_id=project_id, files=["metadata.yaml"]
            )
        )
    )

    for metadata_file in table_metadata_files:
        metadata = Metadata.from_file(metadata_file)

        metadata.deprecated = True
        metadata.deletion_date = deletion_date.date()

        metadata.write(metadata_file)
        click.echo(f"Updated {metadata_file} with deprecation.")

    if not table_metadata_files:
        raise FileNotFoundError(f"No metadata file(s) were found for: {name}")


@metadata.command(help="""
    Validate that deprecation metadata is valid.
    Used by CI to ensure that manual metadata edits have all required info.

    Example:
     ./bqetl metadata validate-deprecation path/to/metadata.yaml path/to/other/metadata.yaml
    """)
@click.argument("metadata_files", nargs=-1)
def validate_deprecation(metadata_files: list[str]):
    """Validate that deprecation metadata is valid."""
    failed_files = set()
    if not metadata_files:
        skip_validation = ConfigLoader.get(
            "metadata", "validation", "skip", fallback=[]
        )
        metadata_files = [
            str(file)
            for file in paths_matching_name_pattern(
                None, "sql", project_id=None, files=["metadata.yaml"]
            )
            if str(file) not in skip_validation
        ]

    for file in metadata_files:
        if Metadata.is_metadata_file(file):
            metadata = Metadata.from_file(file)
            if not validate_deprecation_metadata(metadata, file):
                failed_files.add(file)

    if len(failed_files) > 0:
        click.echo(click.style("Deprecation metadata invalid for:", fg="red"))
        for file in sorted(failed_files):
            click.echo(click.style(str(file), fg="red"))
        raise MetadataValidationError(
            f"Deprecation metadata invalid for: {failed_files}."
        )


@metadata.command(help="""
    Validate workgroup_access and default_table_workgroup_access configurations.

    Example:
     ./bqetl metadata validate-workgroups ga_derived.downloads_with_attribution_v2
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
def validate_workgroups(
    name: str,
    sql_dir: str,
    project_id: str,
):
    """Validate workgroup_access and default_table_workgroup_access configuration."""
    failed_files = set()

    table_metadata_files = list(
        set(
            paths_matching_name_pattern(
                name, sql_dir, project_id=project_id, files=["metadata.yaml"]
            )
        )
    )
    skip_validation = ConfigLoader.get("metadata", "validation", "skip", fallback=[])

    for file in table_metadata_files:
        if str(file) not in skip_validation:
            if Metadata.is_metadata_file(file):
                metadata = Metadata.from_file(file)
                if not validate_workgroup_access(metadata, file):
                    failed_files.add(file)

                if not validate_default_table_workgroup_access(file):
                    failed_files.add(file)

    if len(failed_files) > 0:
        click.echo(click.style("Metadata workgroup validation failed for:", fg="red"))
        for file in failed_files:
            click.echo(click.style(str(file), fg="red"))
        raise MetadataValidationError(
            f"Metadata workgroup validation failed for: {failed_files}"
        )


@metadata.command(help="""
    Get the downstream lineage of a BigQuery table from DataHub.

    Example:
     ./bqetl metadata get-downstream-lineage telemetry_derived.clients_last_seen_v2 --since-days=90
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.option(
    "--since_days",
    "--since-days",
    type=click.IntRange(min=1),
    help="Only follow lineage DataHub has seen in the last N days. Default: all time.",
)
@click.option(
    "--max_degree",
    "--max-degree",
    type=click.Choice(DATAHUB_LINEAGE_DEGREES),
    default=DATAHUB_LINEAGE_DEGREES[-1],
    show_default=True,
    help="Only list assets up to this many hops away. 3+ lists all of them.",
)
@click.option(
    "--type",
    "types",
    multiple=True,
    help="Only list this entity type, e.g. DATASET, CHART, DASHBOARD. Repeatable.",
)
@click.option(
    "--include_usage",
    "--include-usage",
    is_flag=True,
    help="Add query count, user count and last query date (last 30 days) for "
    "BigQuery tables and views.",
)
@click.option(
    "--include_deleted",
    "--include-deleted",
    is_flag=True,
    help="Also list soft-deleted assets.",
)
@click.option(
    "--looker_tmp_days",
    "--looker-tmp-days",
    type=click.IntRange(min=0),
    default=DEFAULT_LOOKER_TMP_DAYS,
    show_default=True,
    help="Drop Looker aggregate table copies (looker_tmp.LR_*) built more than "
    "this many days ago. 0 keeps all.",
)
@click.option(
    "--output_format",
    "--output-format",
    type=click.Choice(["text", "json", "csv"]),
    default="text",
    show_default=True,
    help="Output format.",
)
def get_downstream_lineage(
    name: str,
    project_id: str,
    since_days: Optional[int],
    max_degree: str,
    types: tuple[str, ...],
    include_deleted: bool,
    include_usage: bool,
    looker_tmp_days: int,
    output_format: str,
) -> None:
    """Get the downstream lineage of a BigQuery table from DataHub."""
    parts = name.split(".")
    if len(parts) == 2:
        table = f"{project_id}.{name}"
    elif len(parts) == 3:
        table = name
    else:
        raise click.BadParameter(
            "Expected dataset.table or project.dataset.table", param_hint="NAME"
        )

    wanted_types = {t.upper() for t in types}
    try:
        client = DataHubClient()
        assets = get_downstream_lineage_from_datahub(
            client,
            table,
            since_days=since_days,
            include_deleted=include_deleted,
            include_usage=include_usage,
            looker_tmp_days=looker_tmp_days,
            max_degree=DATAHUB_LINEAGE_DEGREES.index(max_degree) + 1,
        )
    except LoginError as e:
        raise click.ClickException(str(e))
    except AuthError as e:
        raise click.ClickException(
            f"DataHub rejected the token ({e}). If DATAHUB_GMS_TOKEN is set, check "
            "it is valid and not expired."
        )
    except LineageTooLargeError:
        raise click.ClickException(
            "Lineage is too large for one DataHub query (maxRelations limit). "
            "Re-run with --since-days (e.g. --since-days=90) to only follow recent "
            "lineage, or --max-degree=1 or 2 to only list the nearest assets."
        )
    except (requests.RequestException, RuntimeError) as e:
        raise click.ClickException(f"Could not get lineage from DataHub: {e}")

    if wanted_types:
        assets = [a for a in assets if a["type"] in wanted_types]

    fields = [
        "degree",
        "type",
        "subtype",
        "platform",
        "name",
        "qualified_name",
        "urn",
    ]
    if include_usage:
        fields += ["queries_30d", "users_30d", "last_query"]
    if include_deleted:
        fields.append("deleted")
    assets = [{f: a[f] for f in fields} for a in assets]

    if output_format == "json":
        click.echo(
            json.dumps(
                {
                    "table": table,
                    "since_days": since_days,
                    "count": len(assets),
                    "downstream": assets,
                },
                indent=2,
            )
        )
    elif output_format == "csv":
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        writer.writerows(assets)
        click.echo(out.getvalue(), nl=False)
    else:
        click.echo(
            format_text(
                table, assets, show_deleted=include_deleted, show_usage=include_usage
            )
        )

    if since_days:
        click.echo(f"Note: only lineage seen in the last {since_days} days.", err=True)
