# table_workgroup_access_v1

Per-table inventory of **table-specific** **BigQuery Data Viewer**
(`roles/bigquery.dataViewer`) **group** grants across every table and view in
`moz-fx-data-shared-prod`.

One row per table, produced weekly by [`query.py`](query.py) as part of the
`bqetl_data_governance_metadata` DAG. Each run is a full snapshot written to the
run's `collected_at` date partition.

## What it records

For each table/view the job reads the IAM policy and keeps the `group:` members
(users and service accounts are excluded) that hold `roles/bigquery.dataViewer`,
then subtracts the groups the table's **dataset** already grants:

- `google_groups` — the raw group emails granted Data Viewer directly on the
  table and not already granted at the dataset level.
- `workgroups` — those groups parsed to workgroup names, e.g.
  `gcp-wg-ads--data-viewers@firefox.gcp.mozilla.com` → `ads/data-viewers`
  (non-`gcp-wg` groups are omitted from this column).

Most tables have no table-specific access. They still get a row, so this table
remains a complete inventory of every table in the project — the access columns
are just empty:

| `source_table` | `google_groups` |
| --- | --- |
| `main_v5` | `[]` |
| `search_v2` | `[gcp-wg-ads--data-viewers@…]` |

BigQuery cannot store a NULL `ARRAY`, so "nothing specific to this table" reads
back as an **empty array** rather than NULL — filter with
`ARRAY_LENGTH(google_groups) > 0`.

Whether the mozilla-confidential data-viewers workgroup has read access is
recorded per dataset, on
[`dataset_workgroup_access_v1`](../dataset_workgroup_access_v1/README.md).

## Access resolution

`get_iam_policy` on a table returns only bindings set **directly** on that
table; grants inherited from the dataset or project level do not appear there.
Dataset-level grants are resolved once per dataset (via the dataset's access
entries) purely so they can be subtracted here — they are inventoried in their
own right by
[`dataset_workgroup_access_v1`](../dataset_workgroup_access_v1/README.md).

Effective read access for a table is therefore the union of the two tables:

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

If a dataset's own grants cannot be read, every table in it is skipped rather
than recorded — otherwise inherited grants would be misreported as
table-specific. Project- and org-level grants are not resolved.

## Running manually

```bash
# full project snapshot for a given date
python query.py --date 2026-07-24 --max-workers 16

# enumerate what would be inventoried, no writes
python query.py --date 2026-07-24 --dry-run
```

The job makes one `getIamPolicy` call per table. `moz-fx-data-shared-prod`
contains a large number of tables (the `*_stable` / `*_live` datasets alone hold
many thousands), so a full run is API-heavy and takes a while; `--max-workers`
controls concurrency. The executing principal needs `bigquery.tables.getIamPolicy`
on the tables, `bigquery.datasets.get` on the datasets, and `bigquery.jobs.create`.
