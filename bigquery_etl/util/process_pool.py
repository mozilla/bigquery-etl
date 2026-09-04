"""Process pool creation for SQL generators."""

import logging
import multiprocessing
import os
import sys
from contextlib import contextmanager
from uuid import uuid4

from pathos.helpers import mp
from pathos.multiprocessing import ProcessingPool

IMPERSONATE_ENV_VAR = "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"


class _SerialPool:
    """Stand-in for a pool when there isn't enough work to justify one."""

    def map(self, func, *iterables):
        """Apply `func` in this process, matching `ProcessingPool.map`."""
        return list(map(func, *iterables))


def init_worker(log_level: int):
    """Re-establish parent process state that spawned workers don't inherit.

    Forked workers inherited the impersonation wrapper `bqetl` installs over
    `google.auth.default`; spawned workers import `google.auth` fresh and would
    otherwise fall back to the caller's own ADC.
    """
    # Imported here so `bigquery_etl.util.common` isn't pulled in by callers
    # that only need the pool.
    from bigquery_etl.util.common import enable_impersonation

    # The env var is inherited across spawn, and `--no-impersonate` unsets it,
    # so it reflects whether this invocation wants impersonation.
    service_account = os.environ.get(IMPERSONATE_ENV_VAR)
    if service_account:
        enable_impersonation(service_account)
    logging.root.setLevel(log_level)


@contextmanager
def process_pool(parallelism: int, task_count: int, pool_id: str = None):
    """Create a pathos process pool for `task_count` tasks.

    Created as a workaround for process pools crashing and getting stuck on macOS
    in some cases. See https://github.com/mozilla/bigquery-etl/issues/9821 and
    https://github.com/python/cpython/issues/75999.

    Uses at most `parallelism` workers, and runs the tasks in this process
    when there is at most one of them. Workers are started eagerly, and under
    spawn each one re-imports `bqetl`, so sizing the pool to the work matters.

    On macOS, workers that make an HTTP request after the parent process has
    made one segfault in the fork-unsafe parts of CoreFoundation (DNS
    resolution, proxy lookup), and the pool then waits forever on the task the
    dead worker was holding. Spawn avoids this, at the cost of slower worker
    startup and cold caches in the workers.

    Callers must be import-safe: with spawn the workers re-import the entry
    point's `__main__`, so a script that creates the pool at module level
    instead of under `if __name__ == "__main__":` will deadlock.

    pathos caches its pools, keyed by `pool_id`. Each call gets a unique id so
    that pools are never shared: reusing one would run the tasks on workers
    that were created before the start method was set, or with a different
    initializer. Pass `pool_id` only to reuse a pool on purpose.
    """
    if sys.platform == "darwin":
        mp.set_start_method("spawn", force=True)
        multiprocessing.set_start_method("spawn", force=True)

    nodes = min(parallelism, task_count)
    if nodes <= 1:
        yield _SerialPool()
        return

    pool = ProcessingPool(
        nodes,
        id=pool_id if pool_id is not None else uuid4().hex,
        initializer=init_worker,
        initargs=(logging.root.level,),
    )
    try:
        yield pool
    except BaseException:
        pool.terminate()
        raise
    finally:
        # pathos caches pools and doesn't release them when the context exits
        pool.clear()
