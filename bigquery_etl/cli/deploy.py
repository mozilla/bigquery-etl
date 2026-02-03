"""Commands for deploying BigQuery artifacts with dependency resolution."""

import logging
import multiprocessing
import sys
from collections.abc import MutableMapping
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import click
from google.cloud import bigquery

from bigquery_etl.cli.query import _update_query_schema
from bigquery_etl.cli.stage import QUERY_FILE, QUERY_SCRIPT, VIEW_FILE
from bigquery_etl.cli.utils import (
    is_authenticated,
    multi_project_id_option,
    parallelism_option,
    paths_matching_name_pattern,
    respect_dryrun_skip_option,
    sql_dir_option,
    use_cloud_function_option,
)
from bigquery_etl.config import ConfigLoader
from bigquery_etl.dependency import extract_table_references
from bigquery_etl.deploy import (
    FailedDeployException,
    SkippedDeployException,
    SkippedExternalDataException,
    deploy_table,
)
from bigquery_etl.dryrun import get_id_token
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.common import exit_if_running_under_coding_agent, render
from bigquery_etl.util.parallel_topological_sorter import ParallelTopologicalSorter
from bigquery_etl.view import View

log = logging.getLogger(__name__)


@click.command(
    help="""Deploy BigQuery artifacts with dependency resolution.

    This command deploys tables and views with automatic dependency resolution
    and parallel execution. You must specify at least one artifact type to
    deploy using the --tables or --views flags.

    Table-specific options use --table-* prefix, view-specific options use --view-* prefix.

    Coding agents aren't allowed to run this command.

    Examples:

    \b
    # Deploy all tables in a directory
    ./bqetl deploy --tables sql/moz-fx-data-shared-prod/telemetry_derived/

    \b
    # Deploy both tables and views together
    ./bqetl deploy --tables --views telemetry_derived/clients_daily_v6

    \b
    # Deploy tables with force, views with managed label
    ./bqetl deploy --tables --views --table-force --view-add-managed-label telemetry_derived/

    \b
    # Deploy views to different project, skip authorized views
    ./bqetl deploy --views --view-target-project=mozdata --view-skip-authorized telemetry/

    \b
    # Dry run to check dependencies
    ./bqetl deploy --tables --views --dry-run telemetry_derived/

    \b
    # Skip schema updates for tables with existing schema.yaml files
    ./bqetl deploy --tables --table-skip-existing-schemas telemetry_derived/
    """,
)
@click.argument("paths", nargs=-1, required=False)
@click.option(
    "--tables",
    is_flag=True,
    default=False,
    help="Deploy tables (query.sql files)",
)
@click.option(
    "--views",
    is_flag=True,
    default=False,
    help="Deploy views (view.sql files)",
)
@sql_dir_option
@multi_project_id_option(
    default=[ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")]
)
@parallelism_option(default=8)
@click.option(
    "--dry-run",
    "--dry_run",
    is_flag=True,
    default=False,
    help="Validate only, don't deploy",
)
@respect_dryrun_skip_option(default=True)
@use_cloud_function_option
@click.option(
    "--table-force",
    "--table_force",
    is_flag=True,
    default=False,
    help="Deploy tables without validation",
)
@click.option(
    "--table-skip-existing",
    "--table_skip_existing",
    is_flag=True,
    default=False,
    help="Skip updating existing tables",
)
@click.option(
    "--table-skip-external-data",
    "--table_skip_external_data",
    is_flag=True,
    default=False,
    help="Skip publishing external data tables",
)
@click.option(
    "--table-skip-existing-schemas",
    "--table_skip_existing_schemas",
    is_flag=True,
    default=False,
    help="Skip automatic schema updates for tables with existing schema.yaml files. "
    "Tables with allow_field_addition=true will still be updated.",
)
@click.option(
    "--view-force",
    "--view_force",
    is_flag=True,
    default=False,
    help="Deploy views even if there are no changes",
)
@click.option(
    "--view-target-project",
    "--view_target_project",
    required=False,
    help="Target project for views (for cross-project publishing)",
)
@click.option(
    "--view-add-managed-label",
    "--view_add_managed_label",
    is_flag=True,
    default=False,
    help='Add a label "managed" to views for lifecycle management',
)
@click.option(
    "--view-skip-authorized",
    "--view_skip_authorized",
    is_flag=True,
    default=False,
    help="Don't deploy views with labels: {authorized: true} in metadata.yaml",
)
@click.option(
    "--view-authorized-only",
    "--view_authorized_only",
    is_flag=True,
    default=False,
    help="Only deploy views with labels: {authorized: true} in metadata.yaml",
)
def deploy(
    paths,
    tables,
    views,
    sql_dir,
    project_ids,
    parallelism,
    dry_run,
    respect_dryrun_skip,
    use_cloud_function,
    table_force,
    table_skip_existing,
    table_skip_external_data,
    table_skip_existing_schemas,
    view_force,
    view_target_project,
    view_add_managed_label,
    view_skip_authorized,
    view_authorized_only,
):
    """Deploy BigQuery artifacts with dependency resolution."""
    exit_if_running_under_coding_agent()

    if not any([tables, views]):
        raise click.UsageError(
            "Must specify at least one artifact type: --tables or --views"
        )

    if view_skip_authorized and view_authorized_only:
        raise click.UsageError(
            "Cannot use both --view-skip-authorized and --view-authorized-only"
        )

    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    artifact_types = []
    if tables:
        artifact_types.append("table")
    if views:
        artifact_types.append("view")

    credentials = None
    id_token = get_id_token()

    artifacts = _discover_artifacts(paths, sql_dir, project_ids, artifact_types)

    # filter views based on authorized flags
    if view_skip_authorized or view_authorized_only:
        artifacts = _filter_views_by_authorization(
            artifacts, view_skip_authorized, view_authorized_only, id_token
        )

    if not artifacts:
        click.echo("No artifacts found matching the specified criteria.")
        sys.exit(0)

    click.echo(f"Found {len(artifacts)} artifact(s) to deploy.")

    try:
        dependency_graph = _build_dependency_graph(artifacts)
    except Exception as e:
        click.echo(f"Error building dependency graph: {e}", err=True)
        sys.exit(1)

    options = {
        "dry_run": dry_run,
        "respect_dryrun_skip": respect_dryrun_skip,
        "use_cloud_function": use_cloud_function,
        "sql_dir": sql_dir,
        "credentials": credentials,
        "id_token": id_token,
        # Table options
        "table_force": table_force,
        "table_skip_existing": table_skip_existing,
        "table_skip_external_data": table_skip_external_data,
        "table_skip_existing_schemas": table_skip_existing_schemas,
        # View options
        "view_force": view_force,
        "view_target_project": view_target_project,
        "view_add_managed_label": view_add_managed_label,
    }

    results = _execute_deployment(artifacts, dependency_graph, options, parallelism)
    _report_results(results)


def _discover_artifacts(
    paths: Tuple[str],
    sql_dir: str,
    project_ids: List[str],
    artifact_types: List[str],
) -> Dict[str, Tuple[Path, str]]:
    """Find artifacts."""
    artifacts = {}
    patterns = {
        "table": [QUERY_FILE, QUERY_SCRIPT],
        "view": [VIEW_FILE],
    }

    file_patterns = [
        pattern
        for artifact_type in artifact_types
        for pattern in patterns[artifact_type]
    ]

    for project_id in project_ids:
        files = paths_matching_name_pattern(
            paths if paths else None,
            sql_dir,
            project_id,
            file_patterns,
        )

        for file_path in files:
            project, dataset, name = extract_from_query_path(file_path)
            artifact_id = f"{project}.{dataset}.{name}"

            # determine artifact type from file name
            artifact_type = next(
                atype
                for atype, file_names in patterns.items()
                if file_path.name in file_names
            )
            artifacts[artifact_id] = (file_path, artifact_type)

    return artifacts


def _filter_views_by_authorization(
    artifacts: Dict[str, Tuple[Path, str]],
    skip_authorized: bool,
    authorized_only: bool,
    id_token: str,
) -> Dict[str, Tuple[Path, str]]:
    """Filter views based on authorized label in metadata.yaml.

    Matches behavior of bqetl view publish:
    - skip_authorized: Excludes views with {authorized: true}
    - authorized_only: Includes only views with {authorized: true}
    - Views without metadata or without authorized label are treated as not authorized
    """
    filtered_artifacts = {}

    for artifact_id, (file_path, artifact_type) in artifacts.items():
        if artifact_type != "view":
            filtered_artifacts[artifact_id] = (file_path, artifact_type)
            continue

        view = View.from_file(file_path, id_token=id_token)
        is_authorized = (
            view.metadata
            and view.metadata.labels
            and view.metadata.labels.get("authorized") == ""
        )

        if skip_authorized and is_authorized:
            continue
        if authorized_only and not is_authorized:
            continue

        filtered_artifacts[artifact_id] = (file_path, artifact_type)

    return filtered_artifacts


def _build_dependency_graph(
    artifacts: Dict[str, Tuple[Path, str]],
) -> Dict[str, Set[str]]:
    """
    Build dependency graph.

    For tables with schema.yaml, we skip dependency extraction since we're
    deploying the schema structure (not running the query), so circular
    dependencies in the query don't matter.

    For tables with derived_from in metadata:
    - Self-references are excluded (schema derived from parent, not from query)
    - Parent queries are added as dependencies (must be deployed first)

    Returns a dict mapping artifact_id to set of dependencies.
    """
    graph = {}
    id_token = get_id_token()

    for artifact_id, (file_path, artifact_type) in artifacts.items():
        try:
            # extract dependencies based on artifact type
            if artifact_type == "view":
                view = View.from_file(file_path, id_token=id_token)
                references = view.table_references
            elif artifact_type in ["table", "materialized_view"]:
                # for tables with schema.yaml, skip dependency extraction
                schema_file = file_path.parent / SCHEMA_FILE
                if schema_file.exists():
                    references = []
                else:
                    sql_content = render(
                        file_path.name, template_folder=file_path.parent
                    )
                    references = extract_table_references(sql_content)
            else:
                references = []

            dependencies = set()
            for ref in references:
                if ref in artifacts:
                    dependencies.add(ref)

            try:
                metadata = Metadata.of_query_file(file_path)
                if metadata and metadata.schema and metadata.schema.derived_from:
                    # Remove self-references to avoid false circular dependencies
                    dependencies.discard(artifact_id)

                    # Add parent queries as dependencies
                    for derived_from in metadata.schema.derived_from:
                        parent_table_id = ".".join(derived_from.table)
                        if parent_table_id in artifacts:
                            dependencies.add(parent_table_id)
            except Exception:
                pass

            graph[artifact_id] = dependencies

        except Exception as e:
            log.warning(
                f"Could not extract dependencies for {artifact_id}: {e}. "
                "Assuming no dependencies."
            )
            graph[artifact_id] = set()

    return graph


def _execute_deployment(
    artifacts: Dict[str, Tuple[Path, str]],
    dependency_graph: Dict[str, Set[str]],
    options: dict,
    parallelism: int,
) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Deploy artifacts in parallel with dependency ordering.

    Returns a dict mapping artifact_id to (status, error_message).
    Status can be: 'success', 'failed', or 'skipped'.
    """
    manager = multiprocessing.Manager()
    results = manager.dict()

    callback = partial(
        _deploy_artifact_callback,
        artifacts=artifacts,
        options=options,
        results=results,
    )

    try:
        sorter = ParallelTopologicalSorter(
            dependencies=dependency_graph,
            parallelism=parallelism,
        )
        sorter.map(callback)
    except Exception as e:
        click.echo(f"\nError during deployment: {e}", err=True)
        sys.exit(1)

    return dict(results)


def _deploy_artifact_callback(
    artifact_id: str,
    _followup_queue,
    artifacts: Dict[str, Tuple[Path, str]],
    options: dict,
    results: MutableMapping[str, Tuple[str, Optional[str]]],
):
    """
    Deploys a single artifact and tracks results.

    Callback for _execute_deployment.
    """
    file_path, artifact_type = artifacts[artifact_id]

    try:
        if artifact_type == "table":
            _deploy_table_artifact(file_path, options)
        elif artifact_type == "view":
            _deploy_view_artifact(file_path, options)

        results[artifact_id] = ("success", None)
        click.echo(f"✓ {artifact_id}")

    except SkippedDeployException as e:
        results[artifact_id] = ("skipped", str(e))
        click.echo(f"⊘ {artifact_id} (skipped: {e})")
    except SkippedExternalDataException as e:
        results[artifact_id] = ("skipped", str(e))
        click.echo(f"⊘ {artifact_id} (skipped external data)")
    except FailedDeployException as e:
        results[artifact_id] = ("failed", str(e))
        click.echo(f"✗ {artifact_id} (failed: {e})", err=True)
    except Exception as e:
        results[artifact_id] = ("failed", str(e))
        click.echo(f"✗ {artifact_id} (failed: {e})", err=True)


def _needs_schema_update(file_path: Path, skip_existing_schemas: bool = False) -> bool:
    """Check if a table needs schema update.

    Returns true if query schema.yaml is missing or metadata has
    allow_field_addition=true or ALLOW_FIELD_ADDITION in scheduling arguments.

    When skip_existing_schemas is True, only update if schema.yaml is missing
    or has allow_field_addition (matches query schema update --skip-existing behavior).
    """
    if file_path.name != "query.sql":
        return False

    if not skip_existing_schemas:
        return True

    schema_path = file_path.parent / SCHEMA_FILE
    schema_missing = not schema_path.exists()

    # Check if metadata has allow_field_addition
    has_allow_field_addition = False
    try:
        metadata = Metadata.of_query_file(file_path)
        if metadata.schema and metadata.schema.allow_field_addition:
            has_allow_field_addition = True
        elif metadata.scheduling:
            arguments = metadata.scheduling.get("arguments", [])
            if any(
                "--schema_update_option=ALLOW_FIELD_ADDITION" in arg
                for arg in arguments
            ):
                has_allow_field_addition = True
    except Exception:
        pass

    if skip_existing_schemas:
        return schema_missing or has_allow_field_addition
    else:
        return True


def _update_table_schema(file_path: Path, options: dict):
    """Update the schema for a table using the existing query schema update logic."""
    log.info(f"Updating schema for {file_path}")

    try:
        project_id, _, _ = extract_from_query_path(file_path)

        _update_query_schema(
            query_file=file_path,
            sql_dir=options["sql_dir"],
            project_id=project_id,
            tmp_dataset="tmp",  # Default dataset for temporary tables during schema updates
            tmp_tables={},
            use_cloud_function=options["use_cloud_function"],
            respect_dryrun_skip=options["respect_dryrun_skip"],
            is_init=False,
            credentials=options["credentials"],
            id_token=options["id_token"],
            use_dataset_schema=False,
            use_global_schema=False,
        )
    except Exception as e:
        raise FailedDeployException(
            f"Failed to update schema for {file_path}: {e}"
        ) from e


def _deploy_table_artifact(file_path: Path, options: dict):
    """Deploy a table using existing deploy_table function."""
    # Check if schema update is needed before deployment
    if not options["dry_run"] and _needs_schema_update(
        file_path,
        skip_existing_schemas=options.get("table_skip_existing_schemas", False),
    ):
        _update_table_schema(file_path, options)

    if options["dry_run"]:
        schema_path = file_path.parent / SCHEMA_FILE
        try:
            existing_schema = Schema.from_schema_file(schema_path)
        except Exception as e:
            raise SkippedDeployException(
                f"Schema missing for {file_path}. Dry run validation failed."
            ) from e

        # validate schema matches query if not using --table-force
        if not options["table_force"] and str(file_path).endswith(".sql"):
            client = bigquery.Client(credentials=options["credentials"])
            try:
                query_schema = Schema.from_query_file(
                    file_path,
                    use_cloud_function=options["use_cloud_function"],
                    respect_skip=options["respect_dryrun_skip"],
                    sql_dir=options["sql_dir"],
                    client=client,
                    id_token=options["id_token"],
                )
                if not existing_schema.equal(query_schema):
                    dataset_name = file_path.parent.parent.name
                    table_name = file_path.parent.name
                    raise FailedDeployException(
                        f"Query {file_path} does not match schema in {schema_path}. "
                        f"Run `./bqetl query schema update {dataset_name}.{table_name}`"
                    )
            except Exception as e:
                if isinstance(e, (FailedDeployException, SkippedDeployException)):
                    raise
                log.warning(f"Could not validate schema for {file_path}: {e}")
        return

    deploy_table(
        artifact_file=file_path,
        force=options["table_force"],
        skip_existing=options["table_skip_existing"],
        skip_external_data=options["table_skip_external_data"],
        use_cloud_function=options["use_cloud_function"],
        respect_dryrun_skip=options["respect_dryrun_skip"],
        sql_dir=options["sql_dir"],
        credentials=options["credentials"],
        id_token=options["id_token"],
    )


def _deploy_view_artifact(file_path: Path, options: dict):
    """Deploy a view using View.publish method."""
    id_token = options.get("id_token")
    view = View.from_file(file_path, id_token=id_token)

    # Add managed label if requested
    if options.get("view_add_managed_label", False):
        view.labels["managed"] = ""

    if options["dry_run"]:
        if not view.is_valid():
            raise FailedDeployException(f"View validation failed for {file_path}")
        return

    client = bigquery.Client(credentials=options["credentials"])

    success = view.publish(
        target_project=options.get("view_target_project"),
        force=options["view_force"],
        client=client,
        dry_run=False,
    )

    if success is False:
        raise FailedDeployException(f"View publish failed for {file_path}")


def _report_results(results: Dict[str, Tuple[str, Optional[str]]]):
    successes = [k for k, (s, _) in results.items() if s == "success"]
    failures = [k for k, (s, _) in results.items() if s == "failed"]
    skipped = [k for k, (s, _) in results.items() if s == "skipped"]

    click.echo("Deployment Summary:")
    click.echo(f"  ✓ Successful: {len(successes)}")
    click.echo(f"  ✗ Failed: {len(failures)}")
    click.echo(f"  ⊘ Skipped: {len(skipped)}")

    if failures:
        click.echo("\nFailed artifacts:")
        for artifact_id in failures:
            error = results[artifact_id][1]
            click.echo(f"  {artifact_id}: {error}", err=True)
        sys.exit(1)

    click.echo("All deployments completed successfully!")
