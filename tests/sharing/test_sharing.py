"""Tests for the external sharing (BigQuery Sharing) deployment logic."""

from types import SimpleNamespace

from google.api_core.exceptions import NotFound
from google.iam.v1 import policy_pb2

from bigquery_etl.sharing import (
    MANAGED_DESCRIPTION,
    MANAGED_MARKER,
    SUBSCRIBER_ROLE,
    clean_sharing,
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
        self.deleted_exchanges = []
        self.deleted_listings = []

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

    # --- listing / exchange enumeration + deletion (for clean) ---
    def list_data_exchanges(self, parent):
        prefix = f"{parent}/dataExchanges/"
        return [
            SimpleNamespace(name=name, description=getattr(v, "description", "") or "")
            for name, v in self.exchanges.items()
            if name.startswith(prefix)
        ]

    def list_listings(self, parent):
        prefix = f"{parent}/listings/"
        return [
            SimpleNamespace(name=name, description=getattr(v, "description", "") or "")
            for name, v in self.listings.items()
            if name.startswith(prefix)
        ]

    def delete_data_exchange(self, name):
        del self.exchanges[name]
        self.deleted_exchanges.append(name)

    def delete_listing(self, name):
        del self.listings[name]
        self.deleted_listings.append(name)

    def parse_data_exchange_path(self, name):
        return {"data_exchange": name.split("/dataExchanges/")[1].split("/")[0]}

    def parse_listing_path(self, name):
        return {"listing": name.split("/listings/")[1].split("/")[0]}

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


def _metadata(
    subscribers=None,
    exchange="partner_exchange",
    display_name=None,
    description="Test dataset description",
    exchange_id=None,
):
    external_sharing = SimpleNamespace(
        exchange=exchange,
        data_review="https://bugzilla.mozilla.org/1",
        subscribers=subscribers or ["group:partner@example.org"],
        display_name=display_name,
        exchange_id=exchange_id,
    )
    return SimpleNamespace(external_sharing=external_sharing, description=description)


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
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
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
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        client.created_exchanges.clear()
        client.created_listings.clear()
        client.set_policy_calls.clear()
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
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
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        assert "/locations/EU/" in client.created_exchanges[0]

    def test_default_listing_display_name(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata()
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        listing = client.listings[client.created_listings[0]]
        assert listing.display_name == "Mozilla - My Shared"

    def test_configurable_listing_display_name(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(display_name="Partner Feed")
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        listing = client.listings[client.created_listings[0]]
        assert listing.display_name == "Partner Feed"

    def test_exchange_description_from_dataset(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(description="Partner-shared analytics dataset")
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        exchange = client.exchanges[client.created_exchanges[0]]
        assert exchange.description.startswith("Partner-shared analytics dataset")
        # still carries the managed marker so `clean` recognizes it
        assert MANAGED_MARKER in exchange.description

    def test_exchange_id_override(self):
        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        metadata = _metadata(
            exchange="Partner Exchange", exchange_id="custom_exchange_id"
        )
        publish_dataset_sharing(client, bq_client, metadata, "proj", "my_shared")
        assert client.created_exchanges[0].endswith("/dataExchanges/custom_exchange_id")

    def test_exchange_is_private_and_logs_queries(self):
        from google.cloud import bigquery_analyticshub_v1 as analyticshub

        client = FakeAnalyticsHubClient()
        bq_client = FakeBigQueryClient()
        publish_dataset_sharing(client, bq_client, _metadata(), "proj", "my_shared")
        exchange = client.exchanges[client.created_exchanges[0]]
        assert (
            exchange.discovery_type == analyticshub.DiscoveryType.DISCOVERY_TYPE_PRIVATE
        )
        assert exchange.log_linked_dataset_query_user_email is True


PARENT = "projects/p/locations/US"


def _exchange_name(exchange_id):
    return f"{PARENT}/dataExchanges/{exchange_id}"


def _listing_name(exchange_id, listing_id):
    return f"{_exchange_name(exchange_id)}/listings/{listing_id}"


class TestCleanSharing:
    def _seed(self, client, exchange_id, managed_exchange=True):
        client.exchanges[_exchange_name(exchange_id)] = SimpleNamespace(
            description=MANAGED_DESCRIPTION if managed_exchange else "manual"
        )

    def _seed_listing(self, client, exchange_id, listing_id, managed=True):
        client.listings[_listing_name(exchange_id, listing_id)] = SimpleNamespace(
            description=MANAGED_DESCRIPTION if managed else "manual"
        )

    def test_deletes_managed_orphan_listing_and_empty_exchange(self):
        client = FakeAnalyticsHubClient()
        self._seed(client, "ex")
        self._seed_listing(client, "ex", "orphan")

        clean_sharing(client, "p", "US", desired={})

        assert client.deleted_listings == [_listing_name("ex", "orphan")]
        assert client.deleted_exchanges == [_exchange_name("ex")]

    def test_keeps_desired(self):
        client = FakeAnalyticsHubClient()
        self._seed(client, "ex")
        self._seed_listing(client, "ex", "ds1")

        clean_sharing(client, "p", "US", desired={"ex": {"ds1"}})

        assert client.deleted_listings == []
        assert client.deleted_exchanges == []

    def test_keeps_manual_listing_and_its_exchange(self):
        client = FakeAnalyticsHubClient()
        self._seed(client, "ex")
        self._seed_listing(client, "ex", "manual_one", managed=False)

        clean_sharing(client, "p", "US", desired={})

        assert client.deleted_listings == []
        # exchange not empty (manual listing remains) -> kept
        assert client.deleted_exchanges == []

    def test_keeps_manual_exchange_but_deletes_managed_orphan_listing(self):
        client = FakeAnalyticsHubClient()
        self._seed(client, "ex", managed_exchange=False)
        self._seed_listing(client, "ex", "orphan")

        clean_sharing(client, "p", "US", desired={})

        assert client.deleted_listings == [_listing_name("ex", "orphan")]
        # exchange is manual -> never deleted
        assert client.deleted_exchanges == []

    def test_dry_run_deletes_nothing(self):
        client = FakeAnalyticsHubClient()
        self._seed(client, "ex")
        self._seed_listing(client, "ex", "orphan")

        clean_sharing(client, "p", "US", desired={}, dry_run=True)

        assert client.deleted_listings == []
        assert client.deleted_exchanges == []
