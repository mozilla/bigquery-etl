# External Data Sharing (BigQuery Sharing)

Share a dataset with external partners through [BigQuery Sharing](https://cloud.google.com/bigquery/docs/analytics-hub-introduction)
(formerly Analytics Hub) by adding an `external_sharing` block to a dataset's
`dataset_metadata.yaml`.

Sharing is configured at the **dataset level** because a BigQuery Sharing
listing shares a whole dataset. One data exchange and one listing are created
per configured dataset, and the configured groups are granted the
`roles/analyticshub.subscriber` role on the listing so they can subscribe to
(link) the shared dataset in their own project.

The `external_sharing` block declares the desired sharing config in
bigquery-etl; the exchange, listing and subscriber grants are **provisioned by
Terraform in `cloudops-infra`** (alongside the `mozdata` dataset the listing
points at). bigquery-etl owns the metadata schema and its validation only.

## Configuration

```yaml
# sql/moz-fx-data-shared-prod/<dataset>_shared/dataset_metadata.yaml
external_sharing:
  exchange: Partner Exchange            # data exchange display name
  exchange_id: partner_exchange         # optional: explicit exchange resource ID
  display_name: Mozilla - Partner Data  # optional listing display name
  data_review: https://bugzilla.mozilla.org/show_bug.cgi?id=<bug>
  subscribers:
    - group:some-managed-group@mozilla.com
```

## Constraints (enforced by `bqetl metadata validate`)

- **`_shared` datasets only.** `external_sharing` may only be set on a dataset
  whose name ends with `_shared`. These datasets are published to the
  user-facing project (`mozdata`), and the sharing listing points at that copy.
- **`group:` subscribers only.** Every subscriber must be a `group:<email>`
  identity — a Mozilla-managed Google group. This keeps partner user/service
  account emails out of the repo; add those accounts to the group instead.
  Access is scoped by group membership and revoked by removing the group from
  `subscribers`.
- **Authorized views.** Every view in a `_shared` dataset must set
  `labels: {authorized: true}` in its `metadata.yaml`, so the shared listing can
  read through it.
