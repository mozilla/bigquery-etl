"""Find Glean deletion targets without BigQuery.

The table metadata that build_glean_targets needs comes from mozilla-pipeline-schemas
and a generated sql directory instead of BigQuery, so the result is what
find_glean_targets would return once the tables in the sql directory are deployed.
Tables without a schema.yaml, such as most generated tables, can optionally get their
schema from a dry run, which needs dry run access but not BigQuery.
"""

from collections.abc import Collection, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from bigquery_etl.cli.utils import get_glean_app_id_to_app_name_mapping
from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import DryRun, get_id_token
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.schema.stable_table_schema import SchemaFile, get_stable_table_schemas
from bigquery_etl.shredder.config import (
    SHARED_PROD,
    USAGE_DELETION_REQUEST_TABLE,
    DeleteIndex,
    GleanTableInfo,
    build_glean_targets,
    field_paths,
)

SCHEMA_ID_URI_PREFIX = "moz://mozilla.org/schemas/"
# a directory with one of these files is a view, not a table
VIEW_FILES = {"view.sql", "materialized_view.sql"}
# a directory with one of these files and no view file is a table. Tables written by
# something outside the repo only have metadata.yaml or schema.yaml.
METADATA_FILE = "metadata.yaml"
TABLE_FILES = {"query.sql", "query.py", "script.sql", METADATA_FILE, SCHEMA_FILE}
QUERY_FILE = "query.sql"
# the dry run service is the bottleneck, so more workers don't help much
DRY_RUN_PARALLELISM = 8


def schema_id_label(schema_id: str) -> str:
    """Return the schema_id table label for a JSON schema $id.

    e.g. moz://mozilla.org/schemas/glean/ping/1 is labeled glean_ping_1.
    """
    return schema_id.removeprefix(SCHEMA_ID_URI_PREFIX).replace("/", "_")


def stable_table_infos(schemas: Iterable[SchemaFile]) -> list[GleanTableInfo]:
    """Return metadata for the stable tables defined by mozilla-pipeline-schemas.

    Labels are set the way the schema deploy sets them on the tables. Field paths are
    only added for usage_deletion_request_v1, the only stable table that
    build_glean_targets reads columns from.

    get_stable_table_schemas only keeps the newest version of each doctype, so if a
    Glean doctype gets a v2 schema, its _v1 table (which find_glean_targets still
    targets) is missing here. No Glean doctype has a newer version than 1 yet.
    """
    infos = []
    for schema in schemas:
        dataset_id, table_id = schema.stable_table.split(".")
        paths: frozenset[str] = frozenset()
        if table_id == USAGE_DELETION_REQUEST_TABLE:
            paths = field_paths(schema.schema)
        infos.append(
            GleanTableInfo(
                dataset_id=dataset_id,
                table_id=table_id,
                labels={
                    "schema_id": schema_id_label(schema.schema_id),
                    "include_client_id": str(schema.include_client_id).lower(),
                },
                field_paths=paths,
            )
        )
    return infos


def _is_external_table(metadata_file: Path) -> bool:
    """Return whether metadata.yaml defines an external table."""
    try:
        metadata = yaml.safe_load(metadata_file.read_text())
    except yaml.YAMLError:
        return False
    return isinstance(metadata, dict) and metadata.get("external_data") is not None


def is_table_dir(table_dir: Path) -> bool:
    """Return whether a sql directory defines a table.

    Views, materialized views, and external tables aren't tables, the same as
    find_glean_targets only keeping table_type TABLE.
    """
    file_names = {path.name for path in table_dir.iterdir() if path.is_file()}
    if file_names & VIEW_FILES:
        return False
    if METADATA_FILE in file_names and _is_external_table(table_dir / METADATA_FILE):
        return False
    return bool(file_names & TABLE_FILES) or any(
        name.startswith("part") and name.endswith(".sql") for name in file_names
    )


@dataclass
class OfflineGleanTargets:
    """Glean targets found without BigQuery, and the tables whose columns are unknown.

    unresolved maps dataset.table to the reason its columns couldn't be determined,
    for tables in derived datasets with Glean deletion requests. Those tables are
    never targets, so a caller that needs a table's target status has to treat a
    table in unresolved as unknown rather than as not a target.
    """

    targets: DeleteIndex
    unresolved: dict[str, str]


def normalize_table_ids(table_ids: Iterable[str], project: str) -> set[str]:
    """Return table ids in dataset.table form.

    Ids can be dataset.table or project.dataset.table for the given project. Anything
    else raises ValueError, so ids in the wrong form aren't silently ignored.
    """
    normalized = set()
    for table_id in table_ids:
        parts = table_id.split(".")
        if len(parts) == 3 and parts[0] == project:
            parts = parts[1:]
        if len(parts) != 2:
            raise ValueError(
                f"{table_id} must be dataset.table or {project}.dataset.table"
            )
        normalized.add(".".join(parts))
    return normalized


def _dry_run_field_paths(
    query_files: list[Path], sql_dir: Path
) -> list[tuple[frozenset[str], Optional[str]]]:
    """Return the field paths of each query's output from a dry run, and any error.

    Queries on the dry run skip list return no schema, which is reported as an error.
    If there's no dry run identity, every query gets that error so the tables are
    reported as unresolved instead of failing the whole resolution.
    """
    if not query_files:
        return []
    try:
        id_token = get_id_token(ConfigLoader.get("dry_run", "function"))
    except Exception as e:
        return [(frozenset(), f"could not get dry run id token: {e}")] * len(
            query_files
        )

    def dry_run(query_file: Path) -> tuple[frozenset[str], Optional[str]]:
        try:
            schema = DryRun(
                str(query_file), sql_dir=str(sql_dir), id_token=id_token
            ).get_schema()
        except Exception as e:
            return frozenset(), f"dry run failed: {e}"
        if not schema.get("fields"):
            return frozenset(), "dry run returned no schema (skipped or empty)"
        return field_paths(schema["fields"]), None

    with ThreadPoolExecutor(DRY_RUN_PARALLELISM) as pool:
        return list(pool.map(dry_run, query_files))


def derived_table_infos(
    sql_dir: Path,
    project: str,
    dataset_ids: Iterable[str],
    dry_run_tables: Collection[str] = (),
) -> tuple[list[GleanTableInfo], dict[str, str]]:
    """Return metadata for the tables in the given datasets of a sql directory.

    Field paths come from schema.yaml. Tables in dry_run_tables (dataset.table) whose
    schema.yaml is missing or can't be loaded get them from a dry run of query.sql
    instead. Tables whose columns can't be determined are returned with no field
    paths, and with the reason in the second return value.
    """
    infos: list[GleanTableInfo] = []
    unresolved: dict[str, str] = {}
    to_dry_run: list[tuple[int, Path, str]] = []
    for dataset_id in dataset_ids:
        dataset_dir = sql_dir / project / dataset_id
        if not dataset_dir.is_dir():
            continue
        for table_dir in sorted(dataset_dir.iterdir()):
            if not table_dir.is_dir() or not is_table_dir(table_dir):
                continue
            table_id = f"{dataset_id}.{table_dir.name}"
            paths: frozenset[str] = frozenset()
            reason = None
            schema_file = table_dir / SCHEMA_FILE
            if schema_file.is_file():
                try:
                    schema = Schema.from_schema_file(schema_file, sql_dir=sql_dir)
                except Exception as e:
                    reason = f"{SCHEMA_FILE} could not be loaded: {e}"
                else:
                    paths = field_paths(schema.schema.get("fields", []))
            else:
                reason = f"no {SCHEMA_FILE}"

            if reason is not None:
                query_file = table_dir / QUERY_FILE
                if table_id not in dry_run_tables:
                    unresolved[table_id] = f"{reason}, not in dry_run_tables"
                elif not query_file.is_file():
                    unresolved[table_id] = f"{reason}, no {QUERY_FILE} to dry run"
                else:
                    to_dry_run.append((len(infos), query_file, reason))
            infos.append(
                GleanTableInfo(
                    dataset_id=dataset_id, table_id=table_dir.name, field_paths=paths
                )
            )

    results = _dry_run_field_paths([path for _, path, _ in to_dry_run], sql_dir)
    for (index, _, reason), (paths, error) in zip(to_dry_run, results):
        info = infos[index]
        if error is None:
            info.field_paths = paths
        else:
            unresolved[f"{info.dataset_id}.{info.table_id}"] = f"{reason}, {error}"
    return infos, unresolved


def find_glean_targets_offline(
    sql_dir: Path,
    project: str = SHARED_PROD,
    schemas: Optional[Collection[SchemaFile]] = None,
    channel_to_app_name: Optional[Mapping[str, str]] = None,
    dry_run_tables: Collection[str] = (),
) -> OfflineGleanTargets:
    """Return Glean targets like find_glean_targets does, without BigQuery.

    sql_dir is a generated sql directory, e.g. the content of private-generated-sql.
    schemas defaults to get_stable_table_schemas() and channel_to_app_name to the
    probeinfo app listing. mozilla-pipeline-schemas only defines stable tables in
    moz-fx-data-shared-prod, so other projects have no Glean targets.

    Tables in dry_run_tables (dataset.table or project.dataset.table) whose
    schema.yaml is missing or can't be loaded are dry run to get their columns. Dry
    running every such table takes several minutes, so pass only the tables whose
    targets are needed, e.g. the ones changed in a PR.
    """
    if schemas is None:
        schemas = get_stable_table_schemas()
    if channel_to_app_name is None:
        channel_to_app_name = get_glean_app_id_to_app_name_mapping()
    dry_run_tables = normalize_table_ids(dry_run_tables, project)

    stable_tables = stable_table_infos(schemas) if project == SHARED_PROD else []
    unresolved: dict[str, str] = {}

    def load_derived_tables(dataset_ids: list[str]) -> list[GleanTableInfo]:
        infos, dataset_unresolved = derived_table_infos(
            sql_dir, project, dataset_ids, dry_run_tables
        )
        unresolved.update(dataset_unresolved)
        return infos

    def has_view(app_name: str, view_id: str) -> bool:
        return (sql_dir / project / app_name / view_id / "view.sql").is_file()

    targets = build_glean_targets(
        stable_tables,
        load_derived_tables,
        has_view,
        channel_to_app_name,
        project=project,
    )
    return OfflineGleanTargets(targets=targets, unresolved=unresolved)
