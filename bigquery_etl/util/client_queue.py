"""Queue for balancing jobs across billing projects."""

from contextlib import contextmanager
from queue import Queue
import asyncio

from google.cloud import bigquery


class ClientQueue:
    """Queue for balancing jobs across billing projects.

    Also provides default_client for use in operations that do not need to be
    distributed across projects and that may be in excess of parallelism, such
    as copying results from a subset of queries before all queries have
    finished.
    """

    def __init__(self, billing_projects, parallelism):
        """Initialize."""
        clients = [bigquery.Client(project) for project in billing_projects]
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
