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

# Match an `{% if ... is_init() ... %}` block (including the `{%-` whitespace-trim form
# and compound conditions like `{% if is_init() and foo %}`) instead of a bare
# `is_init()`, which can appear in a comment.
IS_INIT_BLOCK_RE = re.compile(r"{%-?\s*if\s+[^%]*\bis_init\(\)")

BACKFILL_DESTINATION_PROJECT = "moz-fx-data-shared-prod"
BACKFILL_DESTINATION_DATASET = "backfills_staging_derived"

# Backfills older than this will not run due to staging table expiration
MAX_BACKFILL_ENTRY_AGE_DAYS = 28

# default retention limit to prevent backfills from accidentally querying empty partitions
NBR_DAYS_RETAINED = 775

# scheduling key naming the date parameter a backfill binds when date_partition_parameter is null
BACKFILL_DATE_PARTITION_PARAMETER = "backfill_date_partition_parameter"


def resolve_date_partition_parameter(
    scheduling: dict, override_depends_on_past_null_partition: Optional[bool] = False
) -> Optional[str]:
    """Resolve the date partition parameter name a backfill binds at run time.

    Mirrors what `bqetl query backfill` does after merging scheduling overrides: the parameter is
    scheduling["date_partition_parameter"], defaulting to submission_date.
    The override_depends_on_past_null_partition path forces submission_date even when metadata
    leaves it null, matching the scheduling override applied in `backfill initiate`.

    scheduling["backfill_date_partition_parameter"] takes precedence over
    date_partition_parameter. It is for tables that leave date_partition_parameter null
    because Airflow targets the partition through a templated destination_table and
    parameter (e.g. hourly runs that rewrite a daily partition), but that still write one
    partition per date and can be backfilled per partition.

    It also wins over a date_partition_parameter set through `bqetl query backfill
    --scheduling-overrides`, since overrides are merged into scheduling before this is
    called. To bind a different parameter, override backfill_date_partition_parameter too.
    """
    if override_depends_on_past_null_partition:
        return "submission_date"
    if backfill_parameter := scheduling.get(BACKFILL_DATE_PARTITION_PARAMETER):
        return backfill_parameter
    return scheduling.get("date_partition_parameter", "submission_date")


def resolve_scheduling_parameters(scheduling: dict) -> List[str]:
    """Return the scheduling parameters a backfill binds alongside the date parameter.

    With backfill_date_partition_parameter set, the scheduling parameter of the same name
    holds the Airflow template for that date. The backfill binds the backfill date in its
    place, so drop it.
    """
    parameters = scheduling.get("parameters", [])
    if backfill_parameter := scheduling.get(BACKFILL_DATE_PARTITION_PARAMETER):
        parameters = [
            parameter
            for parameter in parameters
            if parameter.split(":", 1)[0] != backfill_parameter
        ]
    return parameters


def has_null_date_partition_parameter(scheduling: dict) -> bool:
    """Return True if a backfill writes the whole table rather than one partition per date."""
    return (
        "date_partition_parameter" in scheduling
        and resolve_date_partition_parameter(scheduling) is None
    )


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
    sql_dir, project_id: Optional[str], status: Optional[str] = None
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


def _staging_table_prefix(source_project: str) -> str:
    """Return the table name prefix identifying the project being backfilled.

    Staging tables for every project share one dataset, so a table from a project
    other than the default would otherwise collide with the same dataset.table in
    the default project. Names from the default project are left unprefixed to keep
    existing staging tables (and anything referring to them) unchanged.
    """
    if source_project == BACKFILL_DESTINATION_PROJECT:
        return ""
    return f"{source_project}__"


def get_backfill_staging_qualified_table_name(
    qualified_table_name, entry_date, destination_project: Optional[str] = None
) -> str:
    """Return full table name where processed backfills are stored.

    When a target environment is active, destination_project points the staging
    table at the target project (e.g. a dev sandbox) instead of production.
    """
    source_project, dataset, table = qualified_table_name_matching(qualified_table_name)
    prefix = _staging_table_prefix(source_project)
    backfill_table_id = f"{prefix}{dataset}__{table}_{entry_date}".replace("-", "_")
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
    source_project, dataset, table = qualified_table_name_matching(qualified_table_name)
    prefix = _staging_table_prefix(source_project)
    cloned_table_id = f"{prefix}{dataset}__{table}_backup_{entry_date}".replace(
        "-", "_"
    )
    project = destination_project or BACKFILL_DESTINATION_PROJECT

    return f"{project}.{BACKFILL_DESTINATION_DATASET}.{cloned_table_id}"


def validate_table_metadata(
    sql_dir: str,
    qualified_table_name: str,
    ignore_missing_metadata: bool,
    reinitialize_table: bool = False,
    override_depends_on_past_null_partition: bool = False,
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
        sql_dir,
        qualified_table_name,
        reinitialize_table=reinitialize_table,
        override_depends_on_past_null_partition=override_depends_on_past_null_partition,
    ):
        errors.append(
            f"Tables with depends on past and null partition parameter are not supported: {qualified_table_name}. "
            "Use --reinitialize-table with `bqetl backfill create` to rebuild via the table's is_init() query, "
            "or set override_depends_on_past_null_partition on a custom_query_path entry to backfill it per-partition."
        )

    if not validate_partitioning_type(sql_dir, qualified_table_name):
        errors.append(
            f"Unsupported table partitioning type:  {qualified_table_name}, "
            "only day and month partitioning are supported."
        )

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata = Metadata.from_file(
        Path(sql_dir) / project / dataset / table / METADATA_FILE
    )

    if not validate_backfill_date_partition_parameter(table_metadata):
        errors.append(
            f"{BACKFILL_DATE_PARTITION_PARAMETER} is set on {qualified_table_name} but is "
            "only supported on partitioned tables with date_partition_parameter: null "
            "and no date_partition_offset, and must name a DATE parameter in "
            "scheduling.parameters."
        )

    if not reinitialize_table and not validate_templated_partition_target(
        table_metadata
    ):
        if (table_metadata.scheduling or {}).get(BACKFILL_DATE_PARTITION_PARAMETER):
            errors.append(
                f"{qualified_table_name} has a templated DATE parameter other than the "
                f"one named by {BACKFILL_DATE_PARTITION_PARAMETER}, which backfills can't "
                "render. Backfills only bind that one parameter to each date."
            )
        else:
            errors.append(
                f"{qualified_table_name} targets its partition through a templated "
                "destination_table or date parameter, which backfills can't render. If a "
                "templated DATE parameter holds the date of the partition the query writes "
                "(e.g. hourly runs rewriting a daily partition), set "
                f"scheduling.{BACKFILL_DATE_PARTITION_PARAMETER} to the name of that "
                "parameter. Don't set it when destination_table targets a different period "
                "than the parameter (e.g. the previous month); such tables can't be "
                "backfilled per partition."
            )

    return errors


def validate_depends_on_past(
    sql_dir: str,
    qualified_table_name: str,
    reinitialize_table: bool = False,
    override_depends_on_past_null_partition: bool = False,
) -> bool:
    """Check if the table depends on past and has null date_partition_parameter.

    Fail if depends_on_past=true and date_partition_parameter=null, unless either:
    - the backfill reinitializes the table via its is_init() query
      (reinitialize_table=true), which rebuilds the whole table without depending on
      prior partitions, or
    - override_depends_on_past_null_partition=true, used with a custom_query_path that processes each
      partition independently (so the depends-on-past replay assumption does not apply).
      The custom_query_path requirement is enforced in validate.py.
    """
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata_path = Path(sql_dir) / project / dataset / table / METADATA_FILE

    table_metadata = Metadata.from_file(table_metadata_path)

    if (
        "depends_on_past" in table_metadata.scheduling
        and "date_partition_parameter" in table_metadata.scheduling
    ):
        depends_on_past_null_partition = table_metadata.scheduling[
            "depends_on_past"
        ] and has_null_date_partition_parameter(table_metadata.scheduling)
        if not depends_on_past_null_partition:
            return True
        if override_depends_on_past_null_partition:
            return True
        if reinitialize_table:
            # The reinitialize path rebuilds the whole table from its is_init() query;
            # require that the query actually supports it.
            return query_supports_reinitialize(sql_dir, qualified_table_name)
        return False

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


def validate_backfill_date_partition_parameter(table_metadata: Metadata) -> bool:
    """Check that backfill_date_partition_parameter is only set where it has an effect.

    It replaces a null date_partition_parameter, so it must not be combined with a
    non-null one, and it selects a partition, so the table must be partitioned. It must
    also name a DATE scheduling parameter. Airflow ignores date_partition_offset when
    date_partition_parameter is null but backfills would apply it, so the two can't be
    combined.
    """
    scheduling = table_metadata.scheduling or {}

    backfill_parameter = scheduling.get(BACKFILL_DATE_PARTITION_PARAMETER)
    if not backfill_parameter:
        return True

    names_date_parameter = any(
        len(parts) == 3
        and parts[0] == backfill_parameter
        and parts[1].upper() == "DATE"
        for parts in (
            parameter.split(":", 2) for parameter in scheduling.get("parameters", [])
        )
    )

    return (
        "date_partition_parameter" in scheduling
        and scheduling["date_partition_parameter"] is None
        and table_metadata.bigquery is not None
        and table_metadata.bigquery.time_partitioning is not None
        and names_date_parameter
        and not scheduling.get("date_partition_offset")
    )


def validate_templated_partition_target(table_metadata: Metadata) -> bool:
    """Check that a templated partition target comes with backfill_date_partition_parameter.

    A partitioned table with a null date_partition_parameter may pick its partition with
    Airflow templates in destination_table (e.g. `table${{ ... }}`) or in a DATE
    parameter. Backfills can't render those templates: the parameter fails to parse and
    the templated destination is ignored. backfill_date_partition_parameter tells the
    backfill which parameter to bind to each date instead. It only covers that one
    parameter, so any other templated DATE parameter still fails.
    """
    scheduling = table_metadata.scheduling or {}

    if (
        table_metadata.bigquery is None
        or table_metadata.bigquery.time_partitioning is None
        or "date_partition_parameter" not in scheduling
        or scheduling["date_partition_parameter"] is not None
    ):
        return True

    backfill_parameter = scheduling.get(BACKFILL_DATE_PARTITION_PARAMETER)

    destination_table = scheduling.get("destination_table") or ""
    if (
        not backfill_parameter
        and "$" in destination_table
        and "{{" in destination_table
    ):
        return False

    for parameter in scheduling.get("parameters", []):
        parts = parameter.split(":", 2)
        if len(parts) != 3:
            continue
        name, parameter_type, value = parts
        if name == backfill_parameter:
            continue
        # `bqetl query backfill` only substitutes a literal {{ds}}
        if parameter_type.upper() == "DATE" and "{{" in value and value != "{{ds}}":
            return False

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
    project: Optional[str] = None,
    qualified_table_name: Optional[str] = None,
    status: Optional[str] = None,
    ignore_old_entries: bool = False,
    ignore_missing_metadata: bool = False,
    destination_project: Optional[str] = None,
) -> Dict[str, Backfill]:
    """Return backfill entries to initiate or complete.

    project restricts which project's queries are scanned for backfill entries;
    None scans every project.

    When a target environment is active, destination_project is where the staging
    and backup tables live (the target project), which is checked for existence
    instead of the production backfill project.

    The client runs in the project holding the staging and backup tables, which is
    the backfill project regardless of which project the query itself is in.
    """
    client = bigquery.Client(
        project=destination_project or BACKFILL_DESTINATION_PROJECT
    )

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
