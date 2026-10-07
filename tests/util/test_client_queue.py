import gc

import pytest
from google.auth.credentials import AnonymousCredentials

from bigquery_etl.util import client_queue
from bigquery_etl.util.client_queue import get_client


@pytest.fixture(autouse=True)
def clear_cache():
    client_queue._client.cache_clear()
    yield
    client_queue._client.cache_clear()


class TestGetClient:
    def test_repeated_calls_reuse_one_client(self):
        """Repeated calls with the same arguments should return the same client."""
        credentials = AnonymousCredentials()
        assert get_client(credentials, "p") is get_client(credentials, "p")

    def test_distinct_projects_get_distinct_clients(self):
        """Different projects should get different clients."""
        credentials = AnonymousCredentials()
        assert get_client(credentials, "a") is not get_client(credentials, "b")

    def test_distinct_credentials_get_distinct_clients(self):
        """Different credentials should get different clients."""
        assert get_client(AnonymousCredentials(), "p") is not get_client(
            AnonymousCredentials(), "p"
        )

    def test_keyword_and_positional_calls_agree(self):
        """Passing an argument by keyword should hit the same entry as positionally.

        `cache` keys on the call's argument form, not on the bound signature, so
        `_client(pid, creds, None)` and `_client(pid, credentials=creds)` would be
        separate entries. The `get_client` wrapper exists to normalise that; call
        sites do currently mix the two forms.
        """
        credentials = AnonymousCredentials()
        assert get_client(credentials) is get_client(credentials=credentials)

    def test_each_process_gets_its_own_client(self):
        """Callers in different processes should not share a client.

        A forked child inherits the parent's populated cache, and the inherited
        client's HTTP session wraps sockets the parent still holds. The pid in
        the key is what makes the child build its own instead.
        """
        credentials = AnonymousCredentials()
        assert client_queue._client(1, credentials, "p") is not client_queue._client(
            2, credentials, "p"
        )

    def test_cached_client_keeps_its_credentials_alive(self):
        """A cached client should keep the credentials it was keyed on alive.

        The cache holds a strong reference to its key arguments, so the
        credentials object cannot be collected and have its identity reused by a
        different object while the entry is live.
        """
        credentials = AnonymousCredentials()
        credentials_id = id(credentials)
        client = get_client(credentials, "p")

        del credentials
        gc.collect()

        assert id(client._credentials) == credentials_id
