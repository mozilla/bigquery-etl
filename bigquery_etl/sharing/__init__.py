"""Deploy BigQuery Sharing data exchanges and subscriber grants for external sharing.

`_shared` datasets that declare an `external_sharing` block in their
`dataset_metadata.yaml` are shared with external partners through a BigQuery
Sharing data exchange. These datasets are published to the user-facing project
(mozdata), and the exchange, listing (one per dataset) and subscriber grants
are all set up on that copy.
BigQuery Sharing was formerly named Analytics Hub, so the Google client
library and IAM roles are still named `analyticshub`.

All operations are get-or-create / reconcile so the command is safe to re-run
(e.g. on every scheduled Airflow run).
"""

import re

import click
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Role that lets a principal subscribe to (i.e. link) a BigQuery Sharing listing.
SUBSCRIBER_ROLE = "roles/analyticshub.subscriber"

# Exchanges and listings don't support labels, so bqetl-managed resources are
# tagged with this marker in their description. `sharing clean` only deletes
# resources carrying the marker, so manually-created exchanges/listings are
# never removed.
MANAGED_MARKER = "[managed-by-bqetl]"
_DEFAULT_MANAGED_TEXT = "Managed by bigquery-etl (bqetl sharing). Do not edit manually."


def _managed_description(text=None):
    """Return a resource description carrying the bqetl-managed marker.

    Uses the provided text (e.g. the dataset description) when given, falling
    back to a generic message, and always appends the marker so `sharing clean`
    can recognize bqetl-managed resources.
    """
    base = (text or "").strip() or _DEFAULT_MANAGED_TEXT
    return f"{base} {MANAGED_MARKER}"


MANAGED_DESCRIPTION = _managed_description()


def _is_managed(resource):
    """Return whether a data exchange or listing was created by bqetl."""
    return MANAGED_MARKER in (resource.description or "")


def analytics_hub_client():
    """Return a BigQuery Sharing client."""
    from google.cloud import bigquery_analyticshub_v1 as analyticshub

    return analyticshub.AnalyticsHubServiceClient()


def bigquery_client(project):
    """Return a BigQuery client, used to look up dataset locations."""
    return bigquery.Client(project=project)


def dataset_location(bq_client, project, dataset):
    """Return the BigQuery location of `project.dataset`.

    The data exchange must be created in the same location as the shared
    dataset, so the location is derived from the table rather than configured.
    """
    return bq_client.get_dataset(f"{project}.{dataset}").location


def _sanitize_id(value):
    """Return a valid BigQuery Sharing resource ID derived from `value`.

    Exchange and listing IDs may only contain letters, numbers and
    underscores. Dataset/table names use those already, but be defensive.
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", value)


def _display_name(value):
    """Return a human-readable display name derived from an id.

    Display names allow only letters, numbers, underscores, dashes, ampersands
    and spaces, so turn `telemetry_shared` into `Telemetry Shared`.
    """
    return re.sub(r"[_-]+", " ", value).strip().title()


def ensure_data_exchange(
    client,
    project_id,
    location,
    exchange_id,
    display_name,
    description=None,
    dry_run=False,
):
    """Ensure the data exchange exists and return its resource name."""
    from google.cloud import bigquery_analyticshub_v1 as analyticshub

    parent = client.common_location_path(project_id, location)
    exchange_name = client.data_exchange_path(project_id, location, exchange_id)

    try:
        client.get_data_exchange(name=exchange_name)
        click.echo(f"Data exchange already exists: {exchange_name}")
        return exchange_name
    except NotFound:
        pass

    if dry_run:
        click.echo(f"Dry run; would create data exchange {exchange_name}")
        return exchange_name

    data_exchange = analyticshub.DataExchange(
        display_name=display_name, description=_managed_description(description)
    )
    created = client.create_data_exchange(
        request=analyticshub.CreateDataExchangeRequest(
            parent=parent,
            data_exchange_id=exchange_id,
            data_exchange=data_exchange,
        )
    )
    click.echo(f"Created data exchange: {created.name}")
    return created.name


def ensure_listing(
    client,
    exchange_name,
    listing_id,
    source_project,
    source_dataset,
    display_name,
    dry_run=False,
):
    """Ensure a listing sharing the whole dataset exists and return its name.

    BigQuery Sharing only supports selective (table-level) sharing for data
    clean rooms, so a standard listing shares the entire dataset. The `_shared`
    dataset is the unit of sharing.
    """
    from google.cloud import bigquery_analyticshub_v1 as analyticshub

    listing_name = f"{exchange_name}/listings/{listing_id}"

    try:
        client.get_listing(name=listing_name)
        click.echo(f"Listing already exists: {listing_name}")
        return listing_name
    except NotFound:
        pass

    if dry_run:
        click.echo(f"Dry run; would create listing {listing_name}")
        return listing_name

    dataset_ref = client.dataset_path(source_project, source_dataset)
    bigquery_dataset = analyticshub.Listing.BigQueryDatasetSource(dataset=dataset_ref)
    listing = analyticshub.Listing(
        display_name=display_name,
        description=MANAGED_DESCRIPTION,
        bigquery_dataset=bigquery_dataset,
    )
    created = client.create_listing(
        request=analyticshub.CreateListingRequest(
            parent=exchange_name,
            listing_id=listing_id,
            listing=listing,
        )
    )
    click.echo(f"Created listing: {created.name}")
    return created.name


def grant_subscribers(client, resource_name, subscribers, dry_run=False):
    """Grant `roles/analyticshub.subscriber` on the resource to each subscriber.

    Merges the new members into the existing IAM policy so previously granted
    (or manually added) principals are preserved.
    """
    from google.iam.v1 import iam_policy_pb2

    # Subscribers are `group:<email>` IAM members (validated in parse_metadata).
    members = sorted(set(subscribers))

    try:
        policy = client.get_iam_policy(
            request=iam_policy_pb2.GetIamPolicyRequest(resource=resource_name)
        )
    except NotFound:
        # The listing doesn't exist yet (dry run before creation); every member
        # would be a new grant.
        if dry_run:
            click.echo(
                f"Dry run; would grant {SUBSCRIBER_ROLE} on {resource_name} to "
                f"{', '.join(members)}"
            )
            return
        raise

    binding = next(
        (b for b in policy.bindings if b.role == SUBSCRIBER_ROLE),
        None,
    )
    existing = set(binding.members) if binding else set()
    to_add = [m for m in members if m not in existing]

    if not to_add:
        click.echo(f"Subscribers already up to date on {resource_name}")
        return

    if dry_run:
        click.echo(
            f"Dry run; would grant {SUBSCRIBER_ROLE} on {resource_name} to "
            f"{', '.join(to_add)}"
        )
        return

    if binding is None:
        # `add()` returns the message stored in the repeated field; appending a
        # locally-constructed Binding would store a copy and drop the members.
        binding = policy.bindings.add()
        binding.role = SUBSCRIBER_ROLE
    for member in to_add:
        binding.members.append(member)

    client.set_iam_policy(
        request=iam_policy_pb2.SetIamPolicyRequest(
            resource=resource_name, policy=policy
        )
    )
    click.echo(f"Granted {SUBSCRIBER_ROLE} on {resource_name} to {', '.join(to_add)}")


def publish_dataset_sharing(
    client, bq_client, metadata, project, dataset, dry_run=False
):
    """Reconcile the exchange, listing and subscriber grants for one dataset.

    A standard listing shares the whole dataset, so there is one listing per
    `_shared` dataset. The exchange location is derived from the shared
    dataset's location, since BigQuery Sharing requires the exchange to be
    co-located with the data.
    """
    sharing = metadata.external_sharing
    exchange_id = _sanitize_id(sharing.exchange)
    listing_id = _sanitize_id(dataset)
    location = dataset_location(bq_client, project, dataset)

    click.echo(f"Deploying external sharing for {project}.{dataset} in {location}")

    exchange_name = ensure_data_exchange(
        client,
        project_id=project,
        location=location,
        exchange_id=exchange_id,
        display_name=_display_name(sharing.exchange),
        description=metadata.description,
        dry_run=dry_run,
    )
    listing_name = ensure_listing(
        client,
        exchange_name=exchange_name,
        listing_id=listing_id,
        source_project=project,
        source_dataset=dataset,
        display_name=(sharing.display_name or f"Mozilla - {_display_name(dataset)}"),
        dry_run=dry_run,
    )
    grant_subscribers(client, listing_name, sharing.subscribers, dry_run=dry_run)


def clean_sharing(client, project, location, desired, dry_run=False):
    """Delete bqetl-managed exchanges/listings no longer backed by config.

    Any managed listing not in `desired` is deleted, and any managed exchange
    left with no listings and no desired listings is deleted. Resources without
    the managed marker (manually created) are always left untouched.
    """
    parent = client.common_location_path(project, location)

    exchanges = list(client.list_data_exchanges(parent=parent))
    click.echo(f"Scanning {len(exchanges)} data exchange(s) in {project} ({location})")

    deleted_listings = 0
    deleted_exchanges = 0
    for exchange in exchanges:
        exchange_id = client.parse_data_exchange_path(exchange.name)["data_exchange"]
        desired_listings = desired.get(exchange_id, set())

        remaining = 0
        for listing in client.list_listings(parent=exchange.name):
            listing_id = client.parse_listing_path(listing.name)["listing"]

            if listing_id in desired_listings:
                remaining += 1
                continue
            if not _is_managed(listing):
                # Manually-created listing; leave it (and keep its exchange).
                remaining += 1
                continue

            if dry_run:
                click.echo(f"Dry run; would delete listing {listing.name}")
            else:
                client.delete_listing(name=listing.name)
                click.echo(f"Deleted listing {listing.name}")
            deleted_listings += 1

        # Remove the exchange only if it is managed, now empty, and no config
        # still references it.
        if remaining == 0 and exchange_id not in desired and _is_managed(exchange):
            if dry_run:
                click.echo(f"Dry run; would delete data exchange {exchange.name}")
            else:
                client.delete_data_exchange(name=exchange.name)
                click.echo(f"Deleted data exchange {exchange.name}")
            deleted_exchanges += 1

    verb = "Would delete" if dry_run else "Deleted"
    click.echo(
        f"{verb} {deleted_listings} listing(s) and {deleted_exchanges} exchange(s)."
    )
