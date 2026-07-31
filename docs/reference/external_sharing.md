# External Data Sharing (BigQuery Sharing)

Share a dataset with external partners through [BigQuery Sharing](https://cloud.google.com/bigquery/docs/analytics-hub-introduction)
(formerly Analytics Hub) by adding an `external_sharing` block to a dataset's
`dataset_metadata.yaml`.

Sharing is configured at the **dataset level** because a BigQuery Sharing
listing shares a whole dataset. One data exchange and one listing are created
per configured dataset, and the configured groups are granted the
`roles/analyticshub.subscriber` role on the listing so they can subscribe to
(link) the shared dataset in their own project.

## Configuration

```yaml
# sql/moz-fx-data-shared-prod/<dataset>_shared/dataset_metadata.yaml
external_sharing:
  exchange: Partner Exchange            # data exchange display name
  exchange_id: partner_exchange         # optional: explicit exchange resource ID
                                        # (defaults to a sanitized `exchange`)
  display_name: Mozilla - Partner Data  # optional listing display name
                                        # (defaults to "Mozilla - <dataset>")
  data_review: https://bugzilla.mozilla.org/show_bug.cgi?id=<bug>
  subscribers:
    - group:some-managed-group@mozilla.com
```

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

## Deploying

`external_sharing` is applied by the `bqetl sharing` commands (run from the
`bqetl_artifact_deployment` Airflow DAG after `_shared` views are published to
`mozdata`):

```sh
# Create/reconcile exchanges, listings and subscriber grants
./bqetl sharing deploy '*'

# Remove exchanges/listings no longer backed by config
./bqetl sharing clean
```

Notes:

- **Location** is derived from the shared dataset's BigQuery location; the
  exchange is created co-located with the data. `sharing clean` derives the
  locations to reconcile from the configured datasets, so none needs to be
  passed.
- **Subscriber grants are reconciled** to exactly the configured groups on every
  `deploy`: adding a group grants access, removing one revokes it.
- **Exchange/listing display names and descriptions are set at creation only.**
  Editing them later is not propagated; recreate the resource to change them.
- **`clean` only removes bqetl-managed resources** (tagged with a marker in the
  description). Manually-created exchanges/listings are left untouched.
- Exchanges are created as private (`DISCOVERY_TYPE_PRIVATE`) with
  linked-dataset query logging enabled.

## Testing against a sandbox

Use `--user-facing-project` to target a non-prod project instead of `mozdata`:

```sh
./bqetl sharing deploy <dataset> --user-facing-project moz-fx-data-<sandbox> --dry-run
```
