# dataset_workgroup_access_v1

Per-dataset inventory of **dataset-level** **BigQuery Data Viewer**
(`roles/bigquery.dataViewer`) **group** grants across every dataset in
`moz-fx-data-shared-prod`.

One row per dataset, produced weekly by [`query.py`](query.py) as part of the
`bqetl_data_governance_metadata` DAG. Each run is a full snapshot written to the
run's `collected_at` date partition.

## What it records

For each dataset the job reads its access entries and keeps the `groupByEmail`
entries (users and service accounts are excluded) that hold dataset-level
dataViewer — which BigQuery stores as the legacy `READER` role:

- `google_groups` — the raw group emails with Data Viewer.
- `workgroups` — those groups parsed to workgroup names, e.g.
  `gcp-wg-ads--data-viewers@firefox.gcp.mozilla.com` → `ads/data-viewers`
  (non-`gcp-wg` groups are omitted from this column).
- `mozilla_confidential` — `true` when
  `gcp-wg-mozilla-confidential--data-viewers@firefox.gcp.mozilla.com` is present.

## Relationship to table_workgroup_access_v1

Dataset-level grants apply to every table in the dataset, so they are recorded
once here rather than repeated on every table.
[`table_workgroup_access_v1`](../table_workgroup_access_v1/README.md) records
only the access set **directly** on a table that is not already granted at the
dataset level. Effective read access for a table is the union of the two:

```sql
SELECT
  tbl.source_dataset,
  tbl.source_table,
  ARRAY_CONCAT(ds.workgroups, tbl.workgroups) AS workgroups,
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.table_workgroup_access_v1` AS tbl
JOIN
  `moz-fx-data-shared-prod.data_governance_metadata_derived.dataset_workgroup_access_v1` AS ds
  USING (source_project, source_dataset, collected_at)
```

Project- and org-level grants are not resolved by either job.

## Running manually

```bash
# full project snapshot for a given date
python query.py --date 2026-07-24

# enumerate what would be inventoried, no writes
python query.py --date 2026-07-24 --dry-run
```

The job makes one `datasets.get` call per dataset, so a full run is quick — far
cheaper than the per-table `getIamPolicy` pass in `table_workgroup_access_v1`.
The executing principal needs `bigquery.datasets.get` on the datasets.
