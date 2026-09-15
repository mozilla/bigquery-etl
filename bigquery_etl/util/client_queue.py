"""Ways of getting a BigQuery client.

`ClientQueue` shares a fixed set of clients across threads in one process,
balancing work over billing projects. `get_client` is for the other shape:
worker *processes*, which can't share a client at all (the HTTP session isn't
fork safe and doesn't pickle), so each caches its own.
"""

import asyncio
import os
from contextlib import contextmanager
from queue import Queue
from typing import Dict, Optional, Tuple

from google.cloud import bigquery
from requests import adapters

# Keyed by (pid, project, id(credentials)). The pid keeps a forked child from
# picking up the parent's client, whose HTTP session is not fork safe.
#
# id() is safe as part of the key only because entries are never evicted and a
# bigquery.Client holds a strong reference to its own credentials: the object
# an entry was keyed on therefore outlives the entry, so its address can't be
# freed and reused by a different credentials object. Adding eviction here
# would reintroduce that collision.
_clients: Dict[Tuple[int, Optional[str], int], bigquery.Client] = {}


def get_client(credentials=None, project=None) -> bigquery.Client:
    """Return a BigQuery client, reused for the life of this process.

    Constructing a client costs a few hundred milliseconds of credential setup,
    and the first request on a fresh client also pays a TLS handshake. The
    deploy paths walk thousands of artifacts spread over a pool of worker
    processes, so each worker reuses one client rather than building one per
    artifact.
    """
    key = (os.getpid(), project, id(credentials))
    client = _clients.get(key)
    if client is None:
        client = bigquery.Client(credentials=credentials, project=project)
        _clients[key] = client
    return client


class ClientQueue:
    """Queue for balancing jobs across billing projects.

    Also provides default_client for use in operations that do not need to be
    distributed across projects and that may be in excess of parallelism, such
    as copying results from a subset of queries before all queries have
    finished.
    """

    def __init__(self, billing_projects, parallelism, connection_pool_max_size=None):
        """Initialize.

        connection_pool_max_size sets the pool size in the HTTP adapter of each client in
        the queue. This allows more concurrent requests when a client is shared across threads.
        See https://cloud.google.com/bigquery/docs/python-libraries#troubleshooting_connection_pool_errors
        Increasing connection_pool_max_size will also increase memory usage.
        """
        clients = [bigquery.Client(project) for project in billing_projects]

        if connection_pool_max_size is not None:
            for client in clients:
                adapter = adapters.HTTPAdapter(
                    pool_connections=connection_pool_max_size,
                    pool_maxsize=connection_pool_max_size,
                )
                client._http.mount("https://", adapter)
                client._http._auth_request.session.mount("https://", adapter)

        self.default_client = clients[0]
        self._q = Queue(parallelism)
        for i in range(parallelism):
            self._q.put(clients[i % len(clients)])

    @contextmanager
    def client(self):
        """Context manager for using a client from the queue."""
        client = self._q.get_nowait()
        try:
            yield client
        finally:
            self._q.put_nowait(client)

    def with_client(self, func, *args):
        """Run func with a client from the queue."""
        with self.client() as client:
            return func(client, *args)

    async def async_with_client(self, executor, func, *args):
        """Run func asynchronously in executor."""
        return await asyncio.get_running_loop().run_in_executor(
            executor, self.with_client, func, *args
        )
