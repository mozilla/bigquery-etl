"""Queue for balancing jobs across billing projects."""

import asyncio
from contextlib import contextmanager
from queue import Queue

from google.cloud import bigquery
from requests import adapters


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
