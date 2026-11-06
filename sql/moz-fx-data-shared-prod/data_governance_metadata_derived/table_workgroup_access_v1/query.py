"""Inventory table-specific Data Viewer group grants for every table in a project.

Enumerates every table and view in ``moz-fx-data-shared-prod`` via
region-scoped ``INFORMATION_SCHEMA.TABLES`` and, for each one, reads its IAM
policy to collect the GROUP members (not users or service accounts) that hold
``roles/bigquery.dataViewer`` ("BigQuery Data Viewer"). Each group email is
parsed to a workgroup name where possible
(``gcp-wg-ads--data-viewers@firefox.gcp.mozilla.com`` -> ``ads/data-viewers``).

Writes one row per table to ``table_workgroup_access_v1`` in
``data_governance_metadata_derived``, overwriting the run's ``collected_at``
date partition. Each run is a full snapshot of table-level access.

Access resolution:

* ``get_iam_policy`` on a table returns only bindings set directly on that
  table; dataViewer grants inherited from the dataset (or project) level do NOT
  appear there. This job reports only what is **specific to the table**: the
  directly-granted groups minus the ones its dataset already grants (dataset
  grants are resolved once per dataset). Tables with nothing table-specific
  still get a row, with empty ``google_groups``/``workgroups``.
* Dataset-level access is inventoried separately by
  ``dataset_workgroup_access_v1``, so effective read access for a table is the
  union of its row here and its dataset's row there.
* Project- and org-level grants are not resolved.
"""

import argparse
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

logger = logging.getLogger(__name__)

DATA_VIEWER_ROLE = "roles/bigquery.dataViewer"
WORKGROUP_PREFIX = "gcp-wg-"

_TABLE_WORKGROUP_ACCESS_SCHEMA = [
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("google_groups", "STRING", mode="REPEATED"),
    bigquery.SchemaField("workgroups", "STRING", mode="REPEATED"),
    bigquery.SchemaField("collected_at", "DATE", mode="REQUIRED"),
]


def parse_workgroup(group_email: str) -> Optional[str]:
    """Parse a gcp-wg group email into a workgroup name.

    e.g. gcp-wg-ads--data-viewers@firefox.gcp.mozilla.com -> ads/data-viewers.
    Returns None if the group is not a gcp-wg workgroup group.
    """
    local = group_email.split("@", 1)[0]
    if not local.startswith(WORKGROUP_PREFIX):
        return None
    body = local[len(WORKGROUP_PREFIX) :]
    if "--" not in body:
        return None
    workgroup, role = body.rsplit("--", 1)
    return f"{workgroup}/{role}"


def dataviewer_groups(client: bigquery.Client, table_ref) -> set:
    """Return the set of group emails with dataViewer set directly on a table."""
    policy = client.get_iam_policy(table_ref)
    groups = set()
    for binding in policy.bindings:
        if binding.get("role") != DATA_VIEWER_ROLE:
            continue
        for member in binding.get("members", []):
            if member.startswith("group:"):
                groups.add(member[len("group:") :])
    return groups


def dataset_dataviewer_groups(client: bigquery.Client, dataset_ref) -> set:
    """Return the set of group emails with dataViewer at the dataset level."""
    dataset = client.get_dataset(dataset_ref)
    groups = set()
    for entry in dataset.access_entries:
        # The legacy READER role is BigQuery's dataset-level dataViewer.
        if entry.role == "READER" and entry.entity_type == "groupByEmail":
            groups.add(entry.entity_id)
    return groups


def enumerate_tables(
    client: bigquery.Client, project: str, regions: list[str]
) -> list[tuple[str, str, str]]:
    """Return (dataset, table, table_type) for every table/view in the project.

    Uses region-scoped INFORMATION_SCHEMA.TABLES, which lists every table in
    every dataset in that region in a single query. moz-fx-data-shared-prod
    lives in the US multi-region; pass additional regions if data ever lands
    elsewhere.
    """
    tables: list[tuple[str, str, str]] = []
    for region in regions:
        query = f"""
            SELECT table_schema, table_name, table_type
            FROM `{project}.{region}`.INFORMATION_SCHEMA.TABLES
            ORDER BY table_schema, table_name
        """
        for row in client.query(query).result():
            tables.append((row.table_schema, row.table_name, row.table_type))
    return tables


def resolve_for_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    table_type: str,
    inherited: set,
    collected_at: str,
) -> Optional[dict[str, Any]]:
    """Resolve table-specific dataViewer group access into a destination row.

    ``inherited`` is the dataset's dataViewer group set; groups it already
    grants are subtracted so the row describes only access that is specific to
    this table. When nothing is, the group arrays are empty rather than the row
    being dropped — every table gets a row.

    Returns None when the table's IAM policy can't be read, so the caller can
    skip it rather than record a misleading "no table-specific access" row.
    """
    dataset_ref = bigquery.DatasetReference(project, dataset)
    table_ref = bigquery.TableReference(dataset_ref, table)
    try:
        groups = dataviewer_groups(client, table_ref) - inherited
    except GoogleAPICallError as exc:
        logger.warning(f"IAM read failed for {project}.{dataset}.{table}: {exc}")
        return None

    workgroups = sorted(filter(None, (parse_workgroup(g) for g in groups)))
    return {
        "source_project": project,
        "source_dataset": dataset,
        "source_table": table,
        "table_type": table_type,
        "google_groups": sorted(groups),
        "workgroups": workgroups,
        "collected_at": collected_at,
    }


def save_access(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    date: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
) -> None:
    """Overwrite the run's date partition with all resolved access rows."""
    job_config = bigquery.LoadJobConfig()
    job_config.schema = _TABLE_WORKGROUP_ACCESS_SCHEMA
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="collected_at"
    )
    # Must match the destination table's clustering, otherwise loads into a
    # clustered table fail with "Incompatible table partitioning specification".
    job_config.clustering_fields = ["source_dataset", "source_table"]

    partition_date = date.replace("-", "")
    client.load_table_from_json(
        rows,
        f"{destination_project}.{destination_dataset}.{destination_table}"
        f"${partition_date}",
        job_config=job_config,
    ).result()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", required=True, help="Run date (YYYY-MM-DD); the partition key."
    )
    parser.add_argument("--source-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--regions",
        default="region-us",
        help=(
            "Comma-separated INFORMATION_SCHEMA regions to enumerate "
            "(default: region-us, where moz-fx-data-shared-prod lives)."
        ),
    )
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument("--destination-table", default="table_workgroup_access_v1")
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Enumerate the tables that would be inventoried and exit without writes.",
    )
    args = parser.parse_args()
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be an ISO date (YYYY-MM-DD); got {args.date!r}")
    args.regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    if not args.regions:
        parser.error("--regions must list at least one region (comma-separated).")
    return args


def main() -> None:
    """Snapshot table-specific Data Viewer group access for the whole project."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    client = bigquery.Client(project=args.destination_project)

    tables = enumerate_tables(client, args.source_project, args.regions)
    logger.info(
        f"Enumerated {len(tables)} tables/views in {args.source_project} "
        f"({', '.join(args.regions)})"
    )

    if args.dry_run:
        for dataset, table, table_type in tables[:20]:
            logger.info(f"  would inventory {dataset}.{table} ({table_type})")
        if len(tables) > 20:
            logger.info(f"  ... and {len(tables) - 20} more")
        return

    # Resolve dataset-level grants once per dataset (they apply to every table
    # in the dataset) so they can be subtracted from each table's own grants.
    dataset_inherited: dict[str, set] = {}
    unreadable_datasets: set[str] = set()
    for dataset in sorted({d for d, _, _ in tables}):
        try:
            dataset_inherited[dataset] = dataset_dataviewer_groups(
                client,
                bigquery.DatasetReference(args.source_project, dataset),
            )
        except GoogleAPICallError as exc:
            logger.warning(f"Dataset IAM read failed for {dataset}: {exc}")
            unreadable_datasets.add(dataset)

    rows: list[dict[str, Any]] = []
    skipped = 0
    # Without the dataset's grants there is nothing to subtract, so every
    # inherited grant would be misreported as table-specific. Skip instead.
    if unreadable_datasets:
        skipped += sum(1 for d, _, _ in tables if d in unreadable_datasets)
        logger.warning(
            f"Skipping all tables in {len(unreadable_datasets)} dataset(s) whose "
            f"dataset-level grants could not be read: "
            f"{', '.join(sorted(unreadable_datasets))}"
        )

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                resolve_for_table,
                client,
                args.source_project,
                dataset,
                table,
                table_type,
                dataset_inherited[dataset],
                args.date,
            ): (dataset, table)
            for dataset, table, table_type in tables
            if dataset not in unreadable_datasets
        }
        for future in as_completed(futures):
            dataset, table = futures[future]
            try:
                row = future.result()
            except Exception as e:
                logger.error(f"Worker failed for {dataset}.{table}: {e}")
                skipped += 1
                continue
            if row is None:
                skipped += 1
                continue
            rows.append(row)

    logger.info(f"Resolved {len(rows)} tables; skipped {skipped} due to IAM errors.")

    # A project-wide IAM failure (expired credentials, revoked permission) would
    # otherwise log warnings and exit 0, hiding the failure. Fail loudly when
    # nothing resolved despite tables existing.
    if not rows and tables:
        raise RuntimeError(
            f"Enumerated {len(tables)} tables but resolved 0 IAM policies "
            f"(skipped {skipped}) — failing the task. Likely a credentials or "
            f"permission issue."
        )

    if not rows:
        logger.warning("No rows produced; nothing to write.")
        return

    save_access(
        client,
        rows,
        args.date,
        args.destination_project,
        args.destination_dataset,
        args.destination_table,
    )
    logger.info(
        f"Wrote {len(rows)} rows to "
        f"{args.destination_project}.{args.destination_dataset}."
        f"{args.destination_table}${args.date.replace('-', '')}"
    )


if __name__ == "__main__":
    main()
