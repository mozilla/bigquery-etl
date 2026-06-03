"""Utility functions used by backfills."""

import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from bigquery_etl.backfill.parse import BACKFILL_FILE, Backfill, BackfillStatus
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata, PartitionType
from bigquery_etl.util import extract_from_query_path

QUALIFIED_TABLE_NAME_RE = re.compile(
    r"(?P<project_id>[a-zA-z0-9_-]+)\.(?P<dataset_id>[a-zA-z0-9_-]+)\.(?P<table_id>[a-zA-Z0-9-_$]+)"
)

# Match `{% if is_init() %}`) instead of a bare `is_init()` which can be in a comment
IS_INIT_BLOCK_RE = re.compile(r"{%\s*if\s+is_init\(\)\s*%}")

BACKFILL_DESTINATION_PROJECT = "moz-fx-data-shared-prod"
BACKFILL_DESTINATION_DATASET = "backfills_staging_derived"

# Backfills older than this will not run due to staging table expiration
MAX_BACKFILL_ENTRY_AGE_DAYS = 28

# default retention limit to prevent backfills from accidentally querying empty partitions
NBR_DAYS_RETAINED = 775


def get_effective_retention_days(metadata: Metadata) -> int:
    """Return the effective retention limit in days.

    Uses the smaller of NBR_DAYS_RETAINED and the table's partition expiration_days from
    bigquery.time_partitioning.expiration_days in metadata.yaml, if set.
    """
    retention = NBR_DAYS_RETAINED
    if (
        metadata.bigquery
        and metadata.bigquery.time_partitioning
        and metadata.bigquery.time_partitioning.expiration_days is not None
    ):
        retention = min(
            retention, int(metadata.bigquery.time_partitioning.expiration_days)
        )
    return retention


def get_entries_from_qualified_table_name(
    sql_dir, qualified_table_name, status=None, table_not_exists_ok=False
) -> List[Backfill]:
    """Return backfill entries from qualified table name."""
    backfills = []

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_path = Path(sql_dir) / project / dataset / table

    if not table_path.exists():
        if table_not_exists_ok:
            return []
        click.echo(f"{project}.{dataset}.{table} does not exist")
        sys.exit(1)

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    if backfill_file.exists():
        backfills = Backfill.entries_from_file(backfill_file, status)

    return backfills


def get_qualified_table_name_to_entries_map_by_project(
    sql_dir, project_id: str, status: Optional[str] = None
) -> Dict[str, List[Backfill]]:
    """Return backfill entries from project or all projects if project_id=None is given."""
    backfills_dict: dict = {}

    project_id_glob = project_id if project_id is not None else "*"

    backfill_files = Path(sql_dir).glob(f"{project_id_glob}/*/*/{BACKFILL_FILE}")
    for backfill_file in backfill_files:
        project, dataset, table = extract_from_query_path(backfill_file)
        qualified_table_name = f"{project}.{dataset}.{table}"

        entries = get_entries_from_qualified_table_name(
            sql_dir, qualified_table_name, status
        )

        if entries:
            backfills_dict[qualified_table_name] = entries

    return backfills_dict


def get_backfill_file_from_qualified_table_name(sql_dir, qualified_table_name) -> Path:
    """Return backfill file from qualified table name."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    path = Path(sql_dir)
    query_path = path / project / dataset / table
    backfill_file = query_path / BACKFILL_FILE

    return backfill_file


def get_backfill_staging_qualified_table_name(
    qualified_table_name, entry_date, destination_project: Optional[str] = None
) -> str:
    """Return full table name where processed backfills are stored.

    When a target environment is active, destination_project points the staging
    table at the target project (e.g. a dev sandbox) instead of production.
    """
    _, dataset, table = qualified_table_name_matching(qualified_table_name)
    backfill_table_id = f"{dataset}__{table}_{entry_date}".replace("-", "_")
    project = destination_project or BACKFILL_DESTINATION_PROJECT

    return f"{project}.{BACKFILL_DESTINATION_DATASET}.{backfill_table_id}"


def get_backfill_backup_table_name(
    qualified_table_name: str,
    entry_date: date,
    destination_project: Optional[str] = None,
) -> str:
    """Return full table name where backup of production table is stored.

    When a target environment is active, destination_project points the backup
    table at the target project (e.g. a dev sandbox) instead of production.
    """
    _, dataset, table = qualified_table_name_matching(qualified_table_name)
    cloned_table_id = f"{dataset}__{table}_backup_{entry_date}".replace("-", "_")
    project = destination_project or BACKFILL_DESTINATION_PROJECT

    return f"{project}.{BACKFILL_DESTINATION_DATASET}.{cloned_table_id}"


def validate_table_metadata(
    sql_dir: str,
    qualified_table_name: str,
    ignore_missing_metadata: bool,
    reinitialize_table: bool = False,
) -> List[str]:
    """Run all metadata.yaml validation checks and return list of error strings."""
    if ignore_missing_metadata:
        project, dataset, table = qualified_table_name_matching(qualified_table_name)
        metadata_exists = (
            Path(sql_dir) / project / dataset / table / METADATA_FILE
        ).exists()
        if not metadata_exists:
            print(
                f"Skipping {qualified_table_name} validation because of missing metadata.yaml"
            )
            return []

    errors = []
    if not validate_depends_on_past(
        sql_dir, qualified_table_name, reinitialize_table=reinitialize_table
    ):
        errors.append(
            f"Tables with depends on past and null partition parameter are not supported: {qualified_table_name}. "
            "Use --reinitialize-table with `bqetl backfill create` to rebuild via the table's is_init() query."
        )

    if not validate_partitioning_type(sql_dir, qualified_table_name):
        errors.append(
            f"Unsupported table partitioning type:  {qualified_table_name}, "
            "only day and month partitioning are supported."
        )

    return errors


def validate_depends_on_past(
    sql_dir: str, qualified_table_name: str, reinitialize_table: bool = False
) -> bool:
    """Check if the table depends on past and has null date_partition_parameter.

    Fail if depends_on_past=true and date_partition_parameter=null, unless the
    backfill reinitializes the table via its is_init() query (reinitialize_table=true),
    which rebuilds the whole table without depending on prior partitions.
    """
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata_path = Path(sql_dir) / project / dataset / table / METADATA_FILE

    table_metadata = Metadata.from_file(table_metadata_path)

    if (
        "depends_on_past" in table_metadata.scheduling
        and "date_partition_parameter" in table_metadata.scheduling
    ):
        depends_on_past_null_partition = (
            table_metadata.scheduling["depends_on_past"]
            and table_metadata.scheduling["date_partition_parameter"] is None
        )
        if depends_on_past_null_partition and reinitialize_table:
            # The reinitialize path rebuilds the whole table from its is_init() query;
            # require that the query actually supports it.
            return query_supports_reinitialize(sql_dir, qualified_table_name)
        return not depends_on_past_null_partition

    return True


def query_supports_reinitialize(sql_dir: str, qualified_table_name: str) -> bool:
    """Return True if the table's query.sql can be reinitialized via is_init()."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    query_path = Path(sql_dir) / project / dataset / table / "query.sql"
    if not query_path.exists():
        return False
    return IS_INIT_BLOCK_RE.search(query_path.read_text()) is not None


def validate_partitioning_type(sql_dir: str, qualified_table_name: str) -> bool:
    """Check if the partitioning type is supported."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata_path = Path(sql_dir) / project / dataset / table / METADATA_FILE

    table_metadata = Metadata.from_file(table_metadata_path)

    if table_metadata.bigquery and table_metadata.bigquery.time_partitioning:
        return table_metadata.bigquery.time_partitioning.type in [
            PartitionType.DAY,
            PartitionType.MONTH,
        ]

    return True


def qualified_table_name_matching(qualified_table_name) -> Tuple[str, str, str]:
    """Match qualified table name pattern."""
    if match := QUALIFIED_TABLE_NAME_RE.match(qualified_table_name):
        project_id = match.group("project_id")
        dataset_id = match.group("dataset_id")
        table_id = match.group("table_id")
    else:
        raise AttributeError(
            "Qualified table name must be named like:" + " <project>.<dataset>.<table>"
        )

    return project_id, dataset_id, table_id


def get_scheduled_backfills(
    sql_dir,
    project: str,
    qualified_table_name: Optional[str] = None,
    status: Optional[str] = None,
    ignore_old_entries: bool = False,
    ignore_missing_metadata: bool = False,
    destination_project: Optional[str] = None,
) -> Dict[str, Backfill]:
    """Return backfill entries to initiate or complete.

    When a target environment is active, destination_project is where the staging
    and backup tables live (the target project), which is checked for existence
    instead of the production backfill project.
    """
    client = bigquery.Client(project=destination_project or project)

    if qualified_table_name:
        backfills_dict = {
            qualified_table_name: get_entries_from_qualified_table_name(
                sql_dir, qualified_table_name, status
            )
        }
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project, status
        )

    if ignore_old_entries:
        min_backfill_date = date.today() - timedelta(days=MAX_BACKFILL_ENTRY_AGE_DAYS)
    else:
        min_backfill_date = None

    backfills_to_process_dict = {}

    for qualified_table_name, entries in backfills_dict.items():
        if ignore_missing_metadata:
            project_id, dataset_id, table_id = qualified_table_name_matching(
                qualified_table_name
            )
            if not (
                Path(sql_dir) / project_id / dataset_id / table_id / METADATA_FILE
            ).exists():
                print(
                    f"Skipping backfill for {qualified_table_name} because table metadata is missing."
                )
                continue

        if not entries:
            continue

        if BackfillStatus.INITIATE.value == status and (len(entries)) > 1:
            raise ValueError(
                f"More than one backfill entry for {qualified_table_name} found with status: {status}."
            )

        entry_to_process = entries[0]
        if (
            min_backfill_date is not None
            and entry_to_process.entry_date < min_backfill_date
        ):
            print(
                f"Skipping backfill for {qualified_table_name} because entry date is too old."
            )
            continue

        if (
            BackfillStatus.INITIATE.value == status
            and _should_initiate(
                client, entry_to_process, qualified_table_name, destination_project
            )
        ) or (
            BackfillStatus.COMPLETE.value == status
            and _should_complete(
                client, entry_to_process, qualified_table_name, destination_project
            )
        ):
            backfills_to_process_dict[qualified_table_name] = entry_to_process

    return backfills_to_process_dict


def _should_initiate(
    client: bigquery.Client,
    backfill: Backfill,
    qualified_table_name: str,
    destination_project: Optional[str] = None,
) -> bool:
    """Determine whether a backfill should be initiated.

    Return true if the backfill is in Initiate status and the staging data does not yet exist.
    """
    if backfill.status != BackfillStatus.INITIATE:
        return False

    staging_table = get_backfill_staging_qualified_table_name(
        qualified_table_name, backfill.entry_date, destination_project
    )

    if _table_exists(client, staging_table):
        return False

    return True


def _should_complete(
    client: bigquery.Client,
    backfill: Backfill,
    qualified_table_name: str,
    destination_project: Optional[str] = None,
) -> bool:
    """Determine whether a backfill should be completed.

    Return true if the backfill is in Complete status, the staging data exists, and the backup does not yet exist.
    """
    if backfill.status != BackfillStatus.COMPLETE:
        return False

    staging_table = get_backfill_staging_qualified_table_name(
        qualified_table_name, backfill.entry_date, destination_project
    )
    if not _table_exists(client, staging_table):
        click.echo(f"Backfill staging table does not exist: {staging_table}")
        return False

    backup_table_name = get_backfill_backup_table_name(
        qualified_table_name, backfill.entry_date, destination_project
    )
    if _table_exists(client, backup_table_name):
        click.echo(f"Backfill backup table already exists: {backup_table_name}")
        return False

    return True


def _table_exists(client: bigquery.Client, qualified_table_name: str) -> bool:
    try:
        client.get_table(qualified_table_name)
        return True
    except NotFound:
        return False


def _access_entries_to_bindings(access_entries) -> List[dict]:
    """Convert dataset AccessEntry list to IAM policy bindings.

    Skips entries that don't represent principals (authorized views, routines,
    datasets) or that are project-scoped (specialGroup) and so are already
    granted via project-level IAM.
    """
    # dataset access_entries use legacy names
    legacy_role_to_iam = {
        "READER": "roles/bigquery.dataViewer",
        "WRITER": "roles/bigquery.dataEditor",
        "OWNER": "roles/bigquery.dataOwner",
    }

    entity_type_to_member_prefix = {
        "userByEmail": "user:",
        "groupByEmail": "group:",
        "domain": "domain:",
        "iamMember": "",
    }

    role_to_members: Dict[str, set] = {}
    for entry in access_entries:
        prefix = entity_type_to_member_prefix.get(entry.entity_type)
        if prefix is None:
            continue
        if entry.entity_type == "userByEmail" and entry.entity_id.endswith(
            "gserviceaccount.com"
        ):
            prefix = "serviceAccount:"
        member = f"{prefix}{entry.entity_id}" if prefix else entry.entity_id
        iam_role = legacy_role_to_iam.get(entry.role, entry.role)
        role_to_members.setdefault(iam_role, set()).add(member)
    return [{"role": r, "members": m} for r, m in role_to_members.items()]


def _merge_bindings(*binding_lists) -> List[dict]:
    """Union IAM policies for a bigquery table."""
    merged: Dict[str, set] = {}
    for bindings in binding_lists:
        for b in bindings:
            members = b["members"]
            if not isinstance(members, set):
                members = set(members)
            merged.setdefault(b["role"], set()).update(members)
    return [{"role": r, "members": m} for r, m in merged.items()]


def copy_permissions_to_staging_table(
    client: bigquery.Client,
    prod_qualified_table_name: str,
    staging_qualified_table_name: str,
):
    """Mirror the prod table's IAM and prod dataset's access onto the staging table.

    Reads the prod table's access and access inherited from its dataset and applies
    the union to the staging table (or the backup table).
    """
    prod_project, prod_dataset, _ = qualified_table_name_matching(
        prod_qualified_table_name
    )

    prod_table_policy = client.get_iam_policy(prod_qualified_table_name)
    prod_dataset_obj = client.get_dataset(f"{prod_project}.{prod_dataset}")
    dataset_bindings = _access_entries_to_bindings(prod_dataset_obj.access_entries)

    staging_policy = client.get_iam_policy(staging_qualified_table_name)
    merged_bindings = _merge_bindings(prod_table_policy.bindings, dataset_bindings)

    # print newly added permissions
    click.echo(f"Adding IAM bindings to {staging_qualified_table_name}:")
    existing_by_role = {b["role"]: set(b["members"]) for b in staging_policy.bindings}
    for binding in merged_bindings:
        new_members = set(binding["members"]) - existing_by_role.get(
            binding["role"], set()
        )
        for member in sorted(new_members):
            click.echo(f"+ {binding['role']}: {member}")

    staging_policy.bindings = merged_bindings
    client.set_iam_policy(staging_qualified_table_name, staging_policy)
