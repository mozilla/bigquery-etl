# BigQuery Sharing

Share a dataset with external partners through [BigQuery Sharing](https://cloud.google.com/bigquery/docs/analytics-hub-introduction)
(formerly Analytics Hub) by adding an `external_sharing` block to a dataset's
`dataset_metadata.yaml`. This is distinct from
[public data](public_data.md) sharing, which exposes data publicly rather than
to specific partners.

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
  exchange: Partner Exchange            # data exchange (used for display + ID)
  data_review: https://bugzilla.mozilla.org/show_bug.cgi?id=<bug>
  subscribers:
    - group:some-managed-group@mozilla.com
    - workgroup:some-managed-workgroup/data-viewers

  # all optional:
  exchange_id: partner_exchange              # explicit exchange resource ID
  exchange_display_name: Mozilla - Partner   # exchange display name
  exchange_description: Data shared with …   # exchange description
  listing_id: partner_listing                # explicit listing resource ID
  display_name: Mozilla - Partner Data       # listing display name
                                             # (default: "Mozilla - <dataset>")
  listing_description: …                      # listing description
                                             # (default: dataset description)
  restrict_export: true                      # block export/copy of shared results
```

The optional `*_id` / `*_display_name` / `*_description` fields exist mainly to
**adopt exchanges/listings that were created by hand** (matching their existing
IDs/names) so provisioning doesn't create duplicates.

## Constraints (enforced by `bqetl metadata validate`)

- **`_shared` datasets only.** `external_sharing` may only be set on a dataset
  whose name ends with `_shared`. These datasets are published to the
  user-facing project (`mozdata`), and the sharing listing points at that copy.
- **`group:` or `workgroup:` subscribers only.** Every subscriber must be either
  a `group:<email>` identity (a Mozilla-managed Google group) or a
  `workgroup:<name>` identity (a Mozilla workgroup, e.g.
  `workgroup:some-managed-workgroup/data-viewers`). This keeps partner user/service
  account emails out of the repo; add those accounts to the group or workgroup
  instead. `workgroup:` identities are resolved to IAM members by Terraform in
  cloudops-infra. Access is scoped by group/workgroup membership and revoked by
  removing the identity from `subscribers`.
- **Authorized views.** Every view in a `_shared` dataset must set
  `labels: {authorized: true}` in its `metadata.yaml`, so the shared listing can
  read through it.
