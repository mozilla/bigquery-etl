"""Commands for deploying BigQuery artifacts with dependency resolution."""

import logging
import multiprocessing
import re
import shutil
import sys
from collections import defaultdict
from collections.abc import MutableMapping
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import click
from google.cloud import bigquery

from bigquery_etl.cli.query import _update_query_schema
from bigquery_etl.cli.routine import ROUTINE_FILE_RE
from bigquery_etl.cli.routine import _publish_to_target as _publish_routines_to_target
from bigquery_etl.cli.routine import publish as publish_routines_cmd
from bigquery_etl.cli.utils import (
    defer_option,
    is_authenticated,
    isolated_option,
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
from bigquery_etl.routine.parse_routine import ROUTINE_FILES, RawRoutine
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.common import block_coding_agents, render
from bigquery_etl.util.parallel_topological_sorter import ParallelTopologicalSorter
from bigquery_etl.util.target import (
    MATERIALIZED_VIEW,
    QUERY_FILE,
    QUERY_SCRIPT,
    VIEW_FILE,
    Target,
    collect_target_dependencies,
    ensure_dataset_exists,
    prepare_target_files,
    read_source_identity_from_manifest,
    resolve_partition_for,
)
from bigquery_etl.view import View

log = logging.getLogger(__name__)


@click.command(
    help="""Deploy BigQuery artifacts with dependency resolution.

    This command deploys tables, views, and routines with automatic dependency
    resolution and parallel execution. You must specify at least one artifact
    type to deploy using the --tables, --views, or --routines flags.

    Table-specific options use --table-* prefix, view-specific options use
    --view-* prefix, routine-specific options use --routine-* prefix.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.

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
@block_coding_agents
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
    "--routines",
    is_flag=True,
    default=False,
    help="Deploy routines (udf.sql, stored_procedure.sql files)",
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
@click.option(
    "--routine-dependency-dir",
    "--routine_dependency_dir",
    default=ConfigLoader.get("routine", "dependency_dir"),
    help="The directory JavaScript dependency files for UDFs are stored.",
)
@click.option(
    "--routine-gcs-bucket",
    "--routine_gcs_bucket",
    default=ConfigLoader.get("routine", "publish", "gcs_bucket"),
    help="The GCS bucket where dependency files are uploaded to.",
)
@click.option(
    "--routine-gcs-path",
    "--routine_gcs_path",
    default=ConfigLoader.get("routine", "publish", "gcs_path"),
    help="The GCS path in the bucket where dependency files are uploaded to.",
)
@click.option(
    "--rewrite-tests/--no-rewrite-tests",
    "rewrite_tests",
    default=None,
    help="For --target deploys, copy and rename SQL tests under tests/sql/ to "
    "match the target paths. Defaults to the target's `rewrite_tests` setting "
    "in bqetl_targets.yaml; use this to override per run.",
)
@click.option(
    "--expire-after-hours",
    "--expire_after_hours",
    type=int,
    default=None,
    help="Override the target's default table expiration (in hours). Defaults "
    "to the target's `expire_after_hours` setting in bqetl_targets.yaml.",
)
@click.option(
    "--test-dir",
    "--test_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=None,
    help="Test directory for --rewrite-tests (defaults to tests/sql at repo root).",
)
@defer_option()
@isolated_option()
@click.pass_context
def deploy(
    ctx,
    paths,
    tables,
    views,
    routines,
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
    routine_dependency_dir,
    routine_gcs_bucket,
    routine_gcs_path,
    rewrite_tests,
    expire_after_hours,
    test_dir,
    defer_to_target,
    isolated,
):
    """Deploy BigQuery artifacts with dependency resolution."""
    if not any([tables, views, routines, isolated]):
        raise click.UsageError(
            "Must specify at least one artifact type: --tables, --views, or --routines"
        )

    if view_skip_authorized and view_authorized_only:
        raise click.UsageError(
            "Cannot use both --view-skip-authorized and --view-authorized-only"
        )

    if defer_to_target and isolated:
        raise click.UsageError(
            "--defer-to-target and --isolated are mutually exclusive."
        )

    target = ctx.obj.get("target") if ctx.obj else None

    if isolated and (target is None or target.project_id in project_ids):
        raise click.UsageError(
            "--isolated requires a --target whose project_id differs from "
            "--project-id; otherwise dependency collection and stub generation "
            "are skipped and the deploy will fail on missing target deps."
        )

    # CLI flags override per-run; otherwise fall back to the target's config.
    if target is not None:
        if rewrite_tests is None:
            rewrite_tests = target.rewrite_tests
        if expire_after_hours is None:
            expire_after_hours = target.expire_after_hours
    if rewrite_tests is None:
        rewrite_tests = False

    # The `--isolated` profile: a self-contained mirror into the target
    # project
    if isolated:
        table_force = True
        table_skip_external_data = True
        table_skip_existing_schemas = True
        view_force = True
        routines = True

    if target and view_target_project:
        raise click.UsageError(
            "--view-target-project and --target are mutually exclusive."
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

    artifacts = _discover_artifacts(
        paths,
        sql_dir,
        project_ids,
        artifact_types,
        include_materialized_views=isolated,
    )

    # For --isolated, collect dependencies up front so we can feed any
    # discovered UDFs into the routine publish step below (otherwise the
    # schema-resolver dry-run sees a stale or missing target UDF). Stubs for
    # unmanaged dep tables land directly under sql/<target_project>/...,
    # alongside the regular target artifacts produced by prepare_target_files.
    isolated_routine_deps: List[Path] = []
    if isolated and target and target.project_id not in project_ids:
        isolated_routine_deps = _collect_isolated_dependencies(
            artifacts, sql_dir, target
        )

    # publish routines first since tables/views may depend on them
    routine_results = {}
    cross_project_target = target and target.project_id not in project_ids
    # Used by `rewrite_for_isolated` to filter rewrites: a ref only gets
    # rewritten to its target equivalent if its source identity is in this
    # set. Refs to artifacts not being deployed in this run keep their prod
    # source ref so the dry-run cloud function can resolve them, avoiding
    # the need to stub/deploy the entire transitive dep graph. For stubs
    # (paths under <sql_dir>/<target_project>/...), `extract_from_query_path`
    # would return the target identity; we recover the original source
    # identity from the manifest `_create_target_stub` writes alongside.
    deployed_source_identities: Set[Tuple[str, str, str]] = set()
    for file_path, _ in artifacts.values():
        source_identity = read_source_identity_from_manifest(file_path)
        if source_identity is None:
            source_identity = extract_from_query_path(file_path)
        deployed_source_identities.add(source_identity)
    if cross_project_target and (routines or isolated_routine_deps):
        # Group all routine paths by their source project. `_publish_to_target`
        # is called once per source project so its qualify-non-published-refs
        # logic stays correct — refs to unpublished UDFs need to be qualified
        # with the *source* project they belong to.
        routines_by_source: Dict[str, Set[Path]] = defaultdict(set)
        if routines:
            for f in paths_matching_name_pattern(
                paths if paths else None,
                sql_dir,
                None,
                list(ROUTINE_FILES),
                file_regex=ROUTINE_FILE_RE,
            ):
                if f.name in ROUTINE_FILES:
                    routines_by_source[f.parent.parent.parent.name].add(f)
        for p in isolated_routine_deps:
            routines_by_source[p.parent.parent.parent.name].add(p)

        for files in routines_by_source.values():
            for f in files:
                deployed_source_identities.add(extract_from_query_path(f))

        for source_project in _order_routine_source_projects(routines_by_source):
            files = routines_by_source[source_project]
            result = _publish_routines_to_target(
                target,
                source_project,
                sql_dir,
                routine_dependency_dir,
                routine_gcs_bucket,
                routine_gcs_path,
                defer_to_target,
                isolated,
                routine_files=sorted(files),
                dry_run=dry_run,
                deployed_source_identities=deployed_source_identities,
            )
            if result:
                routine_results.update(result)
    elif routines:
        for project_id in project_ids:
            ctx.invoke(
                publish_routines_cmd,
                project_id=project_id,
                sql_dir=sql_dir,
                dependency_dir=routine_dependency_dir,
                gcs_bucket=routine_gcs_bucket,
                gcs_path=routine_gcs_path,
                dry_run=dry_run,
                defer_to_target=defer_to_target,
                isolated=isolated,
            )

    if target and target.project_id not in project_ids:
        artifacts, source_to_target_paths = _prepare_target_artifacts(
            artifacts,
            target,
            sql_dir,
            defer_to_target,
            isolated,
            deployed_source_identities=deployed_source_identities,
        )

        if rewrite_tests:
            _rewrite_tests_for_target(
                source_to_target_paths,
                test_dir or ConfigLoader.project_dir / "tests" / "sql",
            )

        client = bigquery.Client(project=target.project_id)
        seen_datasets: set = set()
        for project, dataset, _ in (
            extract_from_query_path(f) for f, _ in artifacts.values()
        ):
            dataset_ref = f"{project}.{dataset}"
            if dataset_ref not in seen_datasets:
                seen_datasets.add(dataset_ref)
                ensure_dataset_exists(
                    client,
                    dataset_ref,
                    expiration_hours=expire_after_hours,
                    grant_dryrun_access=target.grant_dryrun_access,
                )

    # filter views based on authorized flags
    if view_skip_authorized or view_authorized_only:
        artifacts = _filter_views_by_authorization(
            artifacts, view_skip_authorized, view_authorized_only, id_token
        )

    if not artifacts:
        if routine_results:
            _report_results(routine_results)
        else:
            click.echo("No artifacts found matching the specified criteria.")
            sys.exit(0)
        return

    click.echo(f"Found {len(artifacts)} artifact(s) to deploy.")

    try:
        dependency_graph = _build_dependency_graph(artifacts)
    except Exception as e:
        click.echo(f"Error building dependency graph: {e}", err=True)
        sys.exit(1)

    options = {
        "dry_run": dry_run,
        "respect_dryrun_skip": respect_dryrun_skip,
        "use_cloud_function": (
            False if target else use_cloud_function
        ),  # cloud function doesn't have access to target project
        "sql_dir": sql_dir,
        "credentials": credentials,
        "id_token": id_token,
        "isolated": isolated,
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
    results.update(routine_results)
    _report_results(results)


def _rewrite_tests_for_target(
    source_to_target_paths: Dict[Path, Path],
    test_dir: Path,
) -> None:
    """Copy and rename SQL tests so they target the deployed artifacts.

    For each (source, target) artifact pair, copies
    `tests/sql/<src_project>/<src_dataset>/<src_table>/` into
    `tests/sql/<tgt_project>/<tgt_dataset>/<tgt_table>/`, then renames any
    fixture files in the new tree whose basenames encode a source identity
    (e.g. `proj.dataset.table.yaml` for input fixtures, or
    `proj.dataset.table.expected.yaml` for the artifact's own expected
    output) to the corresponding target identity. Renaming covers both
    the deployed artifact's own fixtures and input fixtures whose stems
    match any other deployed artifact.
    """
    if not test_dir.exists():
        return

    # Build a lookup keyed by every form of source identity that fixture
    # filenames can use: full `<proj>.<ds>.<tbl>` and short `<ds>.<tbl>`.
    # Maps to the equivalent target identity in full form.
    src_to_tgt_id: Dict[str, str] = {}
    for source_path, target_path in source_to_target_paths.items():
        src = extract_from_query_path(source_path)
        tgt = extract_from_query_path(target_path)
        src_to_tgt_id[".".join(src)] = ".".join(tgt)
        src_to_tgt_id[f"{src[1]}.{src[2]}"] = ".".join(tgt)

    for source_path, target_path in source_to_target_paths.items():
        src = extract_from_query_path(source_path)
        tgt = extract_from_query_path(target_path)
        src_test_dir = test_dir.joinpath(*src)
        if not src_test_dir.exists():
            continue
        tgt_test_dir = test_dir.joinpath(*tgt)
        shutil.copytree(src_test_dir, tgt_test_dir, dirs_exist_ok=True)

        for test_file in tgt_test_dir.rglob("*"):
            if not test_file.is_file():
                continue
            suffix = test_file.suffix
            stem = test_file.name[: -len(suffix)] if suffix else test_file.name
            schema_part = ""
            if stem.endswith(".schema"):
                stem = stem[: -len(".schema")]
                schema_part = ".schema"
            tgt_id = src_to_tgt_id.get(stem)
            if tgt_id is None:
                continue
            new_path = test_file.parent / f"{tgt_id}{schema_part}{suffix}"
            if not new_path.exists():
                test_file.rename(new_path)


def _strip_materialized_view(target_file: Path) -> Path:
    """Convert a materialized view in target dir to query.sql.

    Strips `CREATE MATERIALIZED VIEW … AS` so the artifact deploys as a
    regular table from schema.yaml — matches legacy stage behavior, since
    materialized views can't be recreated without source data access.
    """
    sql_content = target_file.read_text()
    # \bAS\b — match the keyword, not the substring "as" inside identifiers.
    # Under --isolated the staged FQN is rewritten to embed the source dataset
    # name (e.g. `..._firefox_crashreporter_derived__...`), and with
    # IGNORECASE + non-greedy .*? a bare `AS` would match the "as" in "crash"
    # and chop the identifier.
    sql_content = re.sub(
        r"CREATE\s+MATERIALIZED\s+VIEW.*?\bAS\b",
        "",
        sql_content,
        flags=re.DOTALL | re.IGNORECASE,
    )
    new_query_file = target_file.parent / QUERY_FILE
    new_query_file.write_text(sql_content)
    target_file.unlink()
    return new_query_file


def _order_routine_source_projects(
    routines_by_source: Dict[str, Set[Path]],
) -> List[str]:
    """Order source-project routine batches so dependencies publish first.

    `_publish_to_target` is called once per source project, but a routine in
    one project may depend on a routine in another (e.g. a public `mozfun` UDF
    that delegates to a `moz-fx-data-shared-prod` UDF). Publishing the batches
    in arbitrary order can fail with "Function not found" when a dependent is
    published before its dependency. Build a project-level dependency graph from
    each routine's parsed dependencies and return the projects in dependency-first
    order. Within-batch ordering is still handled by `_publish_to_target`.
    """
    # map of routine name (dataset.name) -> source project being published
    name_to_project: Dict[str, str] = {}
    routine_dependencies: Dict[str, List[str]] = {}
    for source_project, files in routines_by_source.items():
        for f in files:
            raw_routine = RawRoutine.from_file(f)
            name_to_project[raw_routine.name] = source_project
            routine_dependencies[raw_routine.name] = raw_routine.dependencies

    # project -> set of other projects providing routines it depends on
    project_edges: Dict[str, Set[str]] = defaultdict(set)
    for routine_name, dependencies in routine_dependencies.items():
        source_project = name_to_project[routine_name]
        for dependency in dependencies:
            dependency_project = name_to_project.get(dependency)
            if dependency_project and dependency_project != source_project:
                project_edges[source_project].add(dependency_project)

    # depth-first topological sort, dependencies before dependents
    ordered: List[str] = []
    state: Dict[str, str] = {}

    def visit(project: str) -> None:
        if state.get(project) == "done":
            return
        if state.get(project) == "visiting":
            # cyclic dependency between projects; fall back to best-effort order
            return
        state[project] = "visiting"
        for dependency_project in sorted(project_edges.get(project, ())):
            visit(dependency_project)
        state[project] = "done"
        ordered.append(project)

    for project in routines_by_source:
        visit(project)
    return ordered


def _collect_isolated_dependencies(
    artifacts: Dict[str, Tuple[Path, str]],
    sql_dir: str,
    target: Target,
) -> List[Path]:
    """Walk dependencies of `artifacts` for an --isolated deploy.

    Mutates `artifacts` to include discovered table/view/MV paths. Stubs for
    unmanaged dep tables are written directly into the target tree
    (sql/<target_project>/...) by `collect_target_dependencies`.
    Returns the list of UDF paths discovered (callers feed these to the
    routine publish step so the schema resolver can dry-run against target).
    """
    routine_deps: List[Path] = []
    existing_paths = {fp for fp, _ in artifacts.values()}
    for dep_path in collect_target_dependencies(existing_paths, sql_dir, target):
        if dep_path.name in (
            QUERY_FILE,
            QUERY_SCRIPT,
            VIEW_FILE,
            MATERIALIZED_VIEW,
        ):
            project, dataset, name = extract_from_query_path(dep_path)
            artifact_type = "view" if dep_path.name == VIEW_FILE else "table"
            artifacts[f"{project}.{dataset}.{name}"] = (dep_path, artifact_type)
        elif dep_path.name == SCHEMA_FILE:
            project, dataset, name = extract_from_query_path(dep_path)
            artifacts[f"{project}.{dataset}.{name}"] = (dep_path, "table")
        elif dep_path.name in ROUTINE_FILES:
            routine_deps.append(dep_path)
    return routine_deps


def _prepare_target_artifacts(
    artifacts: Dict[str, Tuple[Path, str]],
    target: Target,
    sql_dir: str,
    defer_to_target: bool,
    isolated: bool,
    deployed_source_identities: Optional[Set[Tuple[str, str, str]]] = None,
) -> Tuple[Dict[str, Tuple[Path, str]], Dict[Path, Path]]:
    """Copy each artifact into the target tree.

    Per artifact: prepare_target_files (rewrite refs), strip materialized-view
    syntax, and resolve schema.yaml for isolated table deploys.

    Returns a tuple of:
    - artifacts dict re-keyed by target identity
    - source-path → target-path mapping (used by --rewrite-tests; restricted
      to artifacts that survived the INFORMATION_SCHEMA filter)
    """
    new_artifacts: Dict[str, Tuple[Path, str]] = {}
    source_to_target: Dict[Path, Path] = {}
    for _artifact_id, (file_path, artifact_type) in artifacts.items():
        source_project, source_dataset, source_table = extract_from_query_path(
            file_path
        )

        # Skip INFORMATION_SCHEMA artifacts — they're metadata views provided
        # by BigQuery, not deployable artifacts.
        if (
            source_dataset == "INFORMATION_SCHEMA"
            or "INFORMATION_SCHEMA" in source_table
        ):
            continue

        # Stubs from collect_target_dependencies live directly under
        # sql/<target_project>/... — they're already at their final target
        # path so prepare_target_files would needlessly re-render them.
        if source_project == target.project_id:
            target_file = file_path
        else:
            target_files = prepare_target_files(
                [file_path],
                sql_dir,
                source_project,
                target,
                defer_to_target=defer_to_target,
                isolated=isolated,
                auto_deploy=False,
                deployed_source_identities=deployed_source_identities,
            )
            target_file = target_files[0]

        if target_file.name == MATERIALIZED_VIEW:
            target_file = _strip_materialized_view(target_file)

        # Schema resolution for tables without schema.yaml is deferred to
        # `_deploy_table_artifact` so it runs in topo order, after the table's
        # target deps already exist (otherwise the rewritten-query dry-run
        # 404s on dep datasets that haven't been deployed yet).

        project, dataset, name = extract_from_query_path(target_file)
        new_artifacts[f"{project}.{dataset}.{name}"] = (target_file, artifact_type)
        source_to_target[file_path] = target_file
    return new_artifacts, source_to_target


def _resolve_isolated_schema(
    target_file: Path,
    artifact_metadata_path: Path,
    source_project: str,
    source_dataset: str,
    source_table: str,
    sql_dir: str,
) -> None:
    """Ensure target_file's schema.yaml exists for an --isolated table deploy.

    Resolution order (first match wins):
      1. target_file already has a schema.yaml (copied from source) — keep it,
         unless the table declares allow_field_addition (schema may have drifted).
      2. Dry-run the rewritten target query. This is the source of truth for
         the local query's schema; the alternatives below only kick in when
         the dry-run fails (target deps still missing, query unrunnable, etc.).
      3. `SELECT *` dry-run against the prod source table via the dry-run
         cloud function. Falls back to whatever prod currently has. Usable
         when the local query can't be dry-run, but may be stale if local
         changes added fields. Validation downstream will catch a mismatch.
      4. `client.get_table` on source -> same fallback as step 3 but as a
         single metadata RPC. Only works when the runtime credentials have
         read access to source (local dev, not CI's stage SA).

    Raises FailedDeployException if none of the above produces a schema.
    """
    target_schema = target_file.parent / SCHEMA_FILE

    refresh_for_field_addition = False
    if artifact_metadata_path.exists():
        try:
            md = Metadata.from_file(artifact_metadata_path)
            if md.schema and md.schema.allow_field_addition:
                refresh_for_field_addition = True
            elif md.scheduling:
                arguments = md.scheduling.get("arguments", [])
                if any(
                    "--schema_update_option=ALLOW_FIELD_ADDITION" in arg
                    for arg in arguments
                ):
                    refresh_for_field_addition = True
        except Exception:
            pass

    # 1. existing schema.yaml is authoritative unless allow_field_addition.
    # Flatten any `!include` directives so the target schema is self-contained.
    if target_schema.exists() and not refresh_for_field_addition:
        text = target_schema.read_text()
        if "!include" in text:
            Schema.from_yaml(text, Path(sql_dir)).to_yaml_file(target_schema)
        return

    # 2. dry-run the rewritten target query — primary source of truth.
    target_dry_run_err: Optional[Exception] = None
    source_select_err: Optional[Exception] = None
    get_table_err: Optional[Exception] = None
    try:
        Schema.from_query_file(target_file).to_yaml_file(target_schema)
        return
    except Exception as e:
        target_dry_run_err = e
        log.warning(
            f"Target dry-run failed for {source_project}.{source_dataset}."
            f"{source_table}, falling back to source schema ({e})"
        )

    # 3. SELECT * dry-run against the prod source table via cloud function.
    try:
        schema = Schema.for_table(
            project=source_project,
            dataset=source_dataset,
            table=source_table,
            partitioned_by=resolve_partition_for(
                sql_dir, source_project, source_dataset, source_table
            ),
        )
        if schema.schema.get("fields"):
            schema.to_yaml_file(target_schema)
            return
    except Exception as e:
        source_select_err = e
        log.warning(
            f"Source SELECT * dry-run failed for {source_project}.{source_dataset}."
            f"{source_table}, falling back to get_table ({e})"
        )

    # 4. client.get_table on source (local dev fast path).
    try:
        bq_table = bigquery.Client(project=source_project).get_table(
            f"{source_project}.{source_dataset}.{source_table}"
        )
        Schema.from_bigquery_schema(bq_table.schema).to_yaml_file(target_schema)
        return
    except Exception as e:
        get_table_err = e
        log.warning(
            f"get_table failed for {source_project}.{source_dataset}."
            f"{source_table} ({e})"
        )

    raise FailedDeployException(
        f"Cannot resolve schema for {source_project}.{source_dataset}."
        f"{source_table}:\n"
        f"  - target dry-run: {target_dry_run_err}\n"
        f"  - source SELECT * dry-run: {source_select_err}\n"
        f"  - get_table: {get_table_err}\n"
        f"If the query references UDFs you've changed, ensure they're "
        f"discoverable (--routines auto-detects from query refs in --isolated "
        f"mode)."
    )


def _discover_artifacts(
    paths: Tuple[str],
    sql_dir: str,
    project_ids: List[str],
    artifact_types: List[str],
    include_materialized_views: bool = False,
) -> Dict[str, Tuple[Path, str]]:
    """Find artifacts.

    `include_materialized_views` is opt-in because only the --isolated path
    knows how to strip `CREATE MATERIALIZED VIEW` and deploy MVs as schema-
    only tables. Regular deploys skip MV files (matches pre-target behavior).
    """
    artifacts: Dict[str, Tuple[Path, str]] = {}
    patterns = {
        "table": [QUERY_FILE, QUERY_SCRIPT, "script.sql"]
        + ([MATERIALIZED_VIEW] if include_materialized_views else []),
        "view": [VIEW_FILE],
    }

    # Prefer query files when a directory contains multiple definition files (e.g. stage deploys
    # for query.sql manually refreshed materialized views will have a query.sql and script.sql)
    table_priority = {
        QUERY_FILE: 0,
        QUERY_SCRIPT: 1,
        "script.sql": 2,
        MATERIALIZED_VIEW: 3,
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
                (
                    atype
                    for atype, file_names in patterns.items()
                    if file_path.name in file_names
                ),
                None,
            )
            if artifact_type is None:
                log.debug(f"Skipping {file_path}: not a table or view artifact")
                continue

            # don't add if higher priority file is already associated with the path
            existing = artifacts.get(artifact_id)
            if (
                existing is not None
                and existing[1] == "table"
                and artifact_type == "table"
            ):
                if table_priority.get(file_path.name, 10) >= table_priority.get(
                    existing[0].name, 10
                ):
                    continue

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
                elif "*" in ref and ref.replace("*", "wildcard") in artifacts:
                    # A wildcard ref (e.g. `…events_*`) depends on its stub, which
                    # is keyed with `*` replaced by `wildcard` (see
                    # _create_target_stub). Add that edge so the stub table
                    # deploys before the view that selects from it.
                    dependencies.add(ref.replace("*", "wildcard"))

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
    # For --isolated, resolve schema.yaml here (after deps deployed in topo order)
    # rather than upfront in `_prepare_target_artifacts`. Doing it upfront would
    # 404 on rewritten refs that point at target datasets not yet created.
    if (
        options.get("isolated", False)
        and not options["dry_run"]
        and file_path.name in (QUERY_FILE, QUERY_SCRIPT)
    ):
        source_identity = read_source_identity_from_manifest(file_path)
        if source_identity is not None:
            src_project, src_dataset, src_table = source_identity
            _resolve_isolated_schema(
                target_file=file_path,
                artifact_metadata_path=file_path.parent / "metadata.yaml",
                source_project=src_project,
                source_dataset=src_dataset,
                source_table=src_table,
                sql_dir=options["sql_dir"],
            )

    # Check if schema update is needed before deployment.
    # Skip entirely for --isolated: schema update dry-runs the query, but
    # rewritten refs in the isolated mirror point at deps that aren't deployed.
    # The schema.yaml in the target dir is authoritative.
    if (
        not options["dry_run"]
        and not options.get("isolated", False)
        and _needs_schema_update(
            file_path,
            skip_existing_schemas=options.get("table_skip_existing_schemas", False),
        )
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
        isolated=options.get("isolated", False),
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
