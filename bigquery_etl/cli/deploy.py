"""Commands for deploying BigQuery artifacts with dependency resolution."""

import logging
import multiprocessing
import sys
from functools import partial
from pathlib import Path
from typing import Dict, List, Set, Tuple

import click
from google.cloud import bigquery

from bigquery_etl.cli.stage import (
    MATERIALIZED_VIEW,
    QUERY_FILE,
    QUERY_SCRIPT,
    VIEW_FILE,
)
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
from bigquery_etl.schema import SCHEMA_FILE
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.common import render
from bigquery_etl.util.parallel_topological_sorter import ParallelTopologicalSorter
from bigquery_etl.view import View

log = logging.getLogger(__name__)


@click.command(
    help="""Deploy BigQuery artifacts with dependency resolution.

    This command deploys tables, views, and materialized views with automatic
    dependency resolution and parallel execution. You must specify at least one
    artifact type to deploy using the --tables, --views, or --materialized-views flags.

    Examples:

    \b
    # Deploy all tables in a directory
    ./bqetl deploy --tables sql/moz-fx-data-shared-prod/telemetry_derived/

    \b
    # Deploy both tables and views together
    ./bqetl deploy --tables --views telemetry_derived/clients_daily_v6

    \b
    # Deploy with custom parallelism
    ./bqetl deploy --tables --views --parallelism 16 telemetry_derived/

    \b
    # Dry run to check dependencies
    ./bqetl deploy --tables --views --dry-run telemetry_derived/
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
@click.option(
    "--materialized-views",
    "--materialized_views",
    is_flag=True,
    default=False,
    help="Deploy materialized views (materialized_view.sql files)",
)
@sql_dir_option
@multi_project_id_option(
    default=[ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")]
)
@parallelism_option(default=8)
@click.option(
    "--force/--no-force",
    default=False,
    help="Deploy without validation",
)
@click.option(
    "--dry-run",
    "--dry_run",
    is_flag=True,
    default=False,
    help="Validate only, don't deploy",
)
@click.option(
    "--skip-existing",
    "--skip_existing",
    is_flag=True,
    default=False,
    help="Skip updating existing tables",
)
@click.option(
    "--skip-external-data",
    "--skip_external_data",
    is_flag=True,
    default=False,
    help="Skip publishing external data tables",
)
@respect_dryrun_skip_option(default=True)
@use_cloud_function_option
@click.option(
    "--target-project",
    "--target_project",
    required=False,
    help="Target project for views (for cross-project publishing)",
)
def deploy(
    paths,
    tables,
    views,
    materialized_views,
    sql_dir,
    project_ids,
    parallelism,
    force,
    dry_run,
    skip_existing,
    skip_external_data,
    respect_dryrun_skip,
    use_cloud_function,
    target_project,
):
    """Deploy BigQuery artifacts with dependency resolution."""
    if not any([tables, views, materialized_views]):
        raise click.UsageError(
            "Must specify at least one artifact type: "
            "--tables, --views, or --materialized-views"
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
    if materialized_views:
        artifact_types.append("materialized_view")

    credentials = None
    id_token = get_id_token()

    artifacts = _discover_artifacts(paths, sql_dir, project_ids, artifact_types)

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
        "force": force,
        "dry_run": dry_run,
        "skip_existing": skip_existing,
        "skip_external_data": skip_external_data,
        "respect_dryrun_skip": respect_dryrun_skip,
        "use_cloud_function": use_cloud_function,
        "sql_dir": sql_dir,
        "credentials": credentials,
        "id_token": id_token,
        "target_project": target_project,
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
        "materialized_view": [MATERIALIZED_VIEW],
    }

    for artifact_type in artifact_types:
        file_patterns = patterns[artifact_type]

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
                artifacts[artifact_id] = (file_path, artifact_type)

    return artifacts


def _build_dependency_graph(
    artifacts: Dict[str, Tuple[Path, str]],
) -> Dict[str, Set[str]]:
    """
    Build dependency graph.

    For tables with schema.yaml, we skip dependency extraction since we're
    deploying the schema structure (not running the query), so circular
    dependencies in the query don't matter.

    Returns a dict mapping artifact_id to set of dependencies.
    """
    graph = {}

    for artifact_id, (file_path, artifact_type) in artifacts.items():
        try:
            # Extract dependencies based on artifact type
            if artifact_type == "view":
                id_token = get_id_token()
                view = View.from_file(file_path, id_token=id_token)
                references = view.table_references
            elif artifact_type in ["table", "materialized_view"]:
                # For tables with schema.yaml, skip dependency extraction
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
) -> Dict[str, Tuple[str, str]]:
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
    results: dict,
):
    """
    Callback for _execute_deployment.

    Deploys a single artifact and tracks results.
    """
    file_path, artifact_type = artifacts[artifact_id]

    try:
        if artifact_type == "table":
            _deploy_table_artifact(file_path, options)
        elif artifact_type == "view":
            _deploy_view_artifact(file_path, options)
        elif artifact_type == "materialized_view":
            # Deploy materialized views as tables
            _deploy_table_artifact(file_path, options)

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


def _deploy_table_artifact(file_path: Path, options: dict):
    """Deploy a table using existing deploy_table function."""
    from bigquery_etl.schema import SCHEMA_FILE, Schema

    if options["dry_run"]:
        schema_path = file_path.parent / SCHEMA_FILE
        try:
            existing_schema = Schema.from_schema_file(schema_path)
        except Exception as e:
            raise SkippedDeployException(
                f"Schema missing for {file_path}. Dry run validation failed."
            ) from e

        # Validate schema matches query if not using --force
        if not options["force"] and str(file_path).endswith(".sql"):
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
        force=options["force"],
        skip_existing=options["skip_existing"],
        skip_external_data=options["skip_external_data"],
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

    if options["dry_run"]:
        if not view.is_valid():
            raise FailedDeployException(f"View validation failed for {file_path}")
        return

    client = bigquery.Client(credentials=options["credentials"])

    success = view.publish(
        target_project=options.get("target_project"),
        force=options["force"],
        client=client,
        dry_run=False,
    )

    if success is False:
        raise FailedDeployException(f"View publish failed for {file_path}")


def _report_results(results: Dict[str, Tuple[str, str]]):
    """Print deployment summary."""

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
