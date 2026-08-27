# BigQuery Sharing

Share data with, or ingest data from, external partners through
[BigQuery Sharing](https://cloud.google.com/bigquery/docs/analytics-hub-introduction)
(formerly Analytics Hub). Both directions are configured at the **dataset level**
in a dataset's `dataset_metadata.yaml` and are **provisioned by Terraform in
`cloudops-infra`** — bigquery-etl owns the metadata schema and its validation only.

Two directions are supported:

- **[Sharing data out](#sharing-data-with-external-partners)** to partners, via the
  `external_sharing` block.
- **[Subscribing to data](#subscribing-to-external-partner-data)** a partner shares
  with us, via the `external_data_subscription` block.

This is distinct from [public data](public_data.md) sharing, which exposes data
publicly rather than to specific partners.

## Sharing data with external partners

Add an `external_sharing` block to a dataset's `dataset_metadata.yaml`.

Sharing is configured at the **dataset level** because a BigQuery Sharing
listing shares a whole dataset. One data exchange and one listing are created
per configured dataset, and the configured groups are granted the
`roles/analyticshub.subscriber` role on the listing so they can subscribe to
(link) the shared dataset in their own project.

### Configuration

```yaml
# sql/moz-fx-data-shared-prod/<dataset>_shared/dataset_metadata.yaml
external_sharing:
  exchange: Partner Exchange            # data exchange (used for display + ID)
  data_review: https://bugzilla.mozilla.org/show_bug.cgi?id=<bug>
  subscribers:
    - group:some-managed-group@mozilla.com

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

### Constraints (enforced by `bqetl metadata validate`)

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

## Subscribing to external partner data

Subscribe to data an external partner shares with Mozilla by adding an
`external_data_subscription` block to a dataset's `dataset_metadata.yaml`. This is
the **inbound** counterpart of `external_sharing`: subscribing links a
**read-only** dataset (provisioned by BigQuery Sharing) into
`moz-fx-data-shared-prod`, which internal consumers can then query.

Subscribing is configured at the **dataset level** because a subscription links a
whole dataset. One listing subscription is created per configured dataset,
creating the linked dataset, and the configured `readers` are granted
`roles/bigquery.dataViewer` on it.

### Configuration

```yaml
# sql/moz-fx-data-shared-prod/<name>_external/dataset_metadata.yaml
external_data_subscription:
  source_project: "65960090760"   # project hosting the source exchange (id or number)
  data_exchange_id: client_04ea3564_cf0e_4809_bb70_b8912beff9cc  # source exchange ID
  listing_id: mozilla_analytics_cross_channel                    # source listing ID
  readers:
    - workgroup:mozilla-confidential/data-viewers

  # all optional:
  location: us                    # location the linked dataset is created in
                                  # (default: us)
  friendly_name: PMG Analytics    # friendly name of the linked dataset
  description: Data shared with … # description of the linked dataset
  labels:                         # labels applied to the linked dataset
    domain: marketing
```

The `source_project`, `data_exchange_id` and `listing_id` identify the partner's
listing to subscribe to — the partner provides these values.

### Constraints (enforced by `bqetl metadata validate`)

- **`_external` datasets only.** `external_data_subscription` may only be set on a
  dataset whose name ends with `_external`, matching the repo convention for
  externally-sourced datasets (e.g. `acoustic_external`).
- **No tables or views.** A subscription links a read-only dataset, so the dataset
  directory must not define any `query.sql` or `view.sql`. Build derived tables or
  views in a separate dataset that reads from the linked one.
- **`workgroup:` readers only.** Every reader must be a `workgroup:<namespace>/<group>`
  identity (a Mozilla-managed workgroup, e.g. `workgroup:mozilla-confidential/data-viewers`).
  Access is scoped by workgroup membership and revoked by removing the workgroup
  from `readers`.
- **Valid resource IDs.** `data_exchange_id` and `listing_id` must contain only
  letters, numbers and underscores (validated at parse time so a bad value fails
  fast rather than at `terraform apply`).
