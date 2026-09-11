"""Inventory dataset-level BigQuery Data Viewer group grants for a project.

Enumerates every dataset in ``moz-fx-data-shared-prod`` and reads each one's
access entries to collect the GROUP members (not users or service accounts)
that hold dataset-level ``roles/bigquery.dataViewer`` ("BigQuery Data Viewer").
In BigQuery a dataset-level dataViewer grant is stored as the legacy ``READER``
access entry, which is what this job reads. Each group email is parsed to a
workgroup name where possible
(``gcp-wg-ads--data-viewers@firefox.gcp.mozilla.com`` -> ``ads/data-viewers``),
and the row is flagged when the mozilla-confidential data-viewers workgroup is
present.

Writes one row per dataset to ``dataset_workgroup_access_v1`` in
``data_governance_metadata_derived``, overwriting the run's ``collected_at``
date partition. Each run is a full snapshot of dataset-level access.

Dataset-level grants apply to every table in the dataset. The companion
``table_workgroup_access_v1`` records only the access set directly on a table
that is *not* already granted here, so effective read access for a table is the
union of its dataset's row in this table and its own row in that one.

Project- and org-level grants are not resolved.
"""

import argparse
import datetime
import logging
from typing import Any, Optional

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

logger = logging.getLogger(__name__)

MOZILLA_CONFIDENTIAL_GROUP = (
    "gcp-wg-mozilla-confidential--data-viewers@firefox.gcp.mozilla.com"
)
WORKGROUP_PREFIX = "gcp-wg-"

_DATASET_WORKGROUP_ACCESS_SCHEMA = [
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mozilla_confidential", "BOOL", mode="REQUIRED"),
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


def dataset_dataviewer_groups(client: bigquery.Client, dataset_ref) -> set:
    """Return the set of group emails with dataViewer at the dataset level."""
    dataset = client.get_dataset(dataset_ref)
    groups = set()
    for entry in dataset.access_entries:
        # The legacy READER role is BigQuery's dataset-level dataViewer.
        if entry.role == "READER" and entry.entity_type == "groupByEmail":
            groups.add(entry.entity_id)
    return groups


def enumerate_datasets(client: bigquery.Client, project: str) -> list[str]:
    """Return the name of every dataset in the project, sorted."""
    return sorted(dataset.dataset_id for dataset in client.list_datasets(project))


def resolve_for_dataset(
    client: bigquery.Client,
    project: str,
    dataset: str,
    collected_at: str,
) -> Optional[dict[str, Any]]:
    """Resolve dataset-level dataViewer group access into a destination row.

    Returns None when the dataset's access entries can't be read, so the caller
    can skip it rather than record a misleading "no access" row.
    """
    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        groups = dataset_dataviewer_groups(client, dataset_ref)
    except GoogleAPICallError as exc:
        logger.warning(f"Dataset access read failed for {project}.{dataset}: {exc}")
        return None

    return {
        "source_project": project,
        "source_dataset": dataset,
        "mozilla_confidential": MOZILLA_CONFIDENTIAL_GROUP in groups,
        "google_groups": sorted(groups),
        "workgroups": sorted(filter(None, (parse_workgroup(g) for g in groups))),
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
    job_config.schema = _DATASET_WORKGROUP_ACCESS_SCHEMA
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="collected_at"
    )
    # Must match the destination table's clustering, otherwise loads into a
    # clustered table fail with "Incompatible table partitioning specification".
    job_config.clustering_fields = ["source_dataset"]

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
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument("--destination-table", default="dataset_workgroup_access_v1")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Enumerate the datasets that would be inventoried and exit without writes."
        ),
    )
    args = parser.parse_args()
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be an ISO date (YYYY-MM-DD); got {args.date!r}")
    return args


def main() -> None:
    """Snapshot dataset-level Data Viewer group access for the whole project."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    client = bigquery.Client(project=args.destination_project)

    datasets = enumerate_datasets(client, args.source_project)
    logger.info(f"Enumerated {len(datasets)} datasets in {args.source_project}")

    if args.dry_run:
        for dataset in datasets[:20]:
            logger.info(f"  would inventory {dataset}")
        if len(datasets) > 20:
            logger.info(f"  ... and {len(datasets) - 20} more")
        return

    rows: list[dict[str, Any]] = []
    skipped = 0
    for dataset in datasets:
        row = resolve_for_dataset(client, args.source_project, dataset, args.date)
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    logger.info(f"Resolved {len(rows)} datasets; skipped {skipped} due to API errors.")

    # A project-wide failure (expired credentials, revoked permission) would
    # otherwise log warnings and exit 0, hiding the failure. Fail loudly when
    # nothing resolved despite datasets existing.
    if not rows and datasets:
        raise RuntimeError(
            f"Enumerated {len(datasets)} datasets but resolved 0 of them "
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
