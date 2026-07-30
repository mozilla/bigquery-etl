"""Tests for the external sharing (BigQuery Sharing) deployment logic."""

from types import SimpleNamespace

from google.api_core.exceptions import NotFound
from google.iam.v1 import policy_pb2

from bigquery_etl.sharing import (
    SUBSCRIBER_ROLE,
    ensure_data_exchange,
    ensure_listing,
    grant_subscribers,
    publish_dataset_sharing,
)


class FakeAnalyticsHubClient:
    """Minimal in-memory stand-in for AnalyticsHubServiceClient."""

    def __init__(self):
        self.exchanges = {}
        self.listings = {}
        self.policies = {}
        self.created_exchanges = []
        self.created_listings = []
        self.set_policy_calls = []

    def common_location_path(self, project, location):
        return f"projects/{project}/locations/{location}"

    def data_exchange_path(self, project, location, exchange):
        return f"projects/{project}/locations/{location}/dataExchanges/{exchange}"

    def dataset_path(self, project, dataset):
        return f"projects/{project}/datasets/{dataset}"

    def table_path(self, project, dataset, table):
        return f"projects/{project}/datasets/{dataset}/tables/{table}"

    def get_data_exchange(self, name):
        if name not in self.exchanges:
            raise NotFound(name)
        return self.exchanges[name]

    def create_data_exchange(self, request):
        name = f"{request.parent}/dataExchanges/{request.data_exchange_id}"
        self.exchanges[name] = request.data_exchange
        self.created_exchanges.append(name)
        return SimpleNamespace(name=name)

    def get_listing(self, name):
        if name not in self.listings:
            raise NotFound(name)
        return self.listings[name]

    def create_listing(self, request):
        name = f"{request.parent}/listings/{request.listing_id}"
        self.listings[name] = request.listing
        self.created_listings.append(name)
        return SimpleNamespace(name=name)

    # --- IAM ---
    def get_iam_policy(self, request):
        if request.resource not in self.listings:
            raise NotFound(request.resource)
        return self.policies.get(request.resource, policy_pb2.Policy())

    def set_iam_policy(self, request):
        self.policies[request.resource] = request.policy
        self.set_policy_calls.append(request.resource)
        return request.policy


class FakeBigQueryClient:
    """Minimal stand-in for bigquery.Client that reports dataset locations."""

    def __init__(self, location="US"):
        self.location = location

    def get_dataset(self, dataset_ref):
        return SimpleNamespace(location=self.location)


def _metadata(subscribers=None, exchange="partner_exchange", display_name=None):
    external_sharing = SimpleNamespace(
        exchange=exchange,
        data_review="https://bugzilla.mozilla.org/1",
        subscribers=subscribers or ["group:partner@example.org"],
        display_name=display_name,
    )
    return SimpleNamespace(external_sharing=external_sharing)


class TestEnsureDataExchange:
    def test_creates_when_missing(self):
        client = FakeAnalyticsHubClient()
        name = ensure_data_exchange(
            client, "proj", "US", "exch", "Exchange", dry_run=False
        )
        assert name.endswith("/dataExchanges/exch")
        assert client.created_exchanges == [name]

    def test_idempotent_when_present(self):
        client = FakeAnalyticsHubClient()
        ensure_data_exchange(client, "proj", "US", "exch", "Exchange")
        client.created_exchanges.clear()
        ensure_data_exchange(client, "proj", "US", "exch", "Exchange")
        assert client.created_exchanges == []

    def test_dry_run_does_not_create(self):
        client = FakeAnalyticsHubClient()
        ensure_data_exchange(client, "proj", "US", "exch", "Exchange", dry_run=True)
        assert client.created_exchanges == []


class TestEnsureListing:
    def test_creates_when_missing(self):
        client = FakeAnalyticsHubClient()
        exchange = "projects/proj/locations/US/dataExchanges/exch"
        name = ensure_listing(client, exchange, "ds", "proj", "ds", "ds")
        assert name == f"{exchange}/listings/ds"
        assert client.created_listings == [name]

    def test_dry_run_does_not_create(self):
        client = FakeAnalyticsHubClient()
        exchange = "projects/proj/locations/US/dataExchanges/exch"
        ensure_listing(client, exchange, "ds", "proj", "ds", "ds", dry_run=True)
        assert client.created_listings == []


class TestGrantSubscribers:
    def _listing(self, client):
        exchange = "projects/proj/locations/US/dataExchanges/exch"
        return ensure_listing(client, exchange, "ds", "proj", "ds", "ds")

    def test_grants_new_members(self):
        client = FakeAnalyticsHubClient()
        listing = self._listing(client)
        grant_subscribers(client, listing, ["group:a@example.org"])
        policy = client.policies[listing]
        binding = policy.bindings[0]
        assert binding.role == SUBSCRIBER_ROLE
        assert "group:a@example.org" in binding.members

    def test_idempotent_when_already_granted(self):
        client = FakeAnalyticsHubClient()
        listing = self._listing(client)
        grant_subscribers(client, listing, ["group:a@example.org"])
        client.set_policy_calls.clear()
        grant_subscribers(client, listing, ["group:a@example.org"])
        assert client.set_policy_calls == []

    def test_merges_with_existing_members(self):
        client = FakeAnalyticsHubClient()
        listing = self._listing(client)
        grant_subscribers(client, listing, ["group:a@example.org"])
        grant_subscribers(client, listing, ["group:b@example.org"])
        members = client.policies[listing].bindings[0].members
        assert set(members) == {"group:a@example.org", "group:b@example.org"}

    def test_dry_run_does_not_set_policy(self):
        client = FakeAnalyticsHubClient()
        listing = self._listing(client)
        grant_subscribers(client, listing, ["group:a@example.org"], dry_run=True)
        assert client.set_policy_calls == []

    def test_dry_run_before_listing_exists(self):
        client = FakeAnalyticsHubClient()
        # listing not created -> get_iam_policy raises NotFound
        grant_subscribers(
            client,
            "projects/proj/locations/US/dataExchanges/exch/listings/missing",
            ["group:a@example.org"],
            dry_run=True,
        )
        assert client.set_policy_calls == []


class TestPublishDatasetSharing:
    def test_full_reconcile(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(subscribers=["group:partner@example.org"])
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        assert len(client.created_exchanges) == 1
        assert len(client.created_listings) == 1
        listing = client.created_listings[0]
        assert (
            "group:partner@example.org" in client.policies[listing].bindings[0].members
        )

    def test_rerun_is_idempotent(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(subscribers=["group:partner@example.org"])
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        client.created_exchanges.clear()
        client.created_listings.clear()
        client.set_policy_calls.clear()
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        assert client.created_exchanges == []
        assert client.created_listings == []
        assert client.set_policy_calls == []

    def test_dry_run_makes_no_changes(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata()
        publish_dataset_sharing(
            client,
            bq_client,
            metadata,
            "proj",
            "my_shared",
            dry_run=True,
        )
        assert client.created_exchanges == []
        assert client.created_listings == []
        assert client.set_policy_calls == []

    def test_location_derived_from_dataset(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient(location="EU")
        metadata = _metadata()
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        assert "/locations/EU/" in client.created_exchanges[0]

    def test_default_listing_display_name(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata()
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        listing = client.listings[client.created_listings[0]]
        assert listing.display_name == "Mozilla - My Shared"

    def test_configurable_listing_display_name(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(display_name="Partner Feed")
        publish_dataset_sharing(
            client, bq_client, metadata, "proj", "my_shared"
        )
        listing = client.listings[client.created_listings[0]]
        assert listing.display_name == "Partner Feed"
