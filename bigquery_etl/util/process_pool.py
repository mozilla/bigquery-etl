"""Process pool creation for SQL generators."""

import logging
import multiprocessing
import os
import sys
from contextlib import contextmanager
from uuid import uuid4

from pathos.helpers import mp
from pathos.multiprocessing import ProcessingPool


class _SerialResult:
    """Stand-in for the async result `amap` and `apipe` return."""

    def __init__(self, func):
        self._value = None
        self._exception = None
        try:
            self._value = func()
        except Exception as e:
            # Async maps report failures from `get()`, not from the call that
            # dispatched the work, so hold the exception until then.
            self._exception = e

    def get(self, timeout=None):
        """Return the result, or raise the exception the work raised."""
        if self._exception is not None:
            raise self._exception
        return self._value

    def ready(self):
        """Return whether the work is done, which it always is here."""
        return True

    def wait(self, timeout=None):
        """Do nothing, since the work already ran."""

    def successful(self):
        """Return whether the work completed without raising."""
        return self._exception is None


class _SerialPool:
    """Stand-in for a pool when there isn't enough work to justify one.

    Implements the map and pipe interfaces of `ProcessingPool` so that callers
    behave the same either way. Note that `ProcessingPool` has no `starmap`;
    pass tuples to `map` and unpack them in the mapped function.
    """

    def map(self, func, *iterables):
        """Apply `func` in this process, matching `ProcessingPool.map`."""
        return list(map(func, *iterables))

    def imap(self, func, *iterables):
        """Apply `func` lazily, matching `ProcessingPool.imap`."""
        return map(func, *iterables)

    def uimap(self, func, *iterables):
        """Apply `func` lazily, matching `ProcessingPool.uimap`.

        Results stay in order, which callers of an unordered map must tolerate
        anyway.
        """
        return map(func, *iterables)

    def amap(self, func, *iterables):
        """Apply `func` now, returning a result object like `ProcessingPool.amap`."""
        return _SerialResult(lambda: list(map(func, *iterables)))

    def pipe(self, func, *args, **kwargs):
        """Call `func` in this process, matching `ProcessingPool.pipe`."""
        return func(*args, **kwargs)

    def apipe(self, func, *args, **kwargs):
        """Call `func` now, returning a result object like `ProcessingPool.apipe`."""
        return _SerialResult(lambda: func(*args, **kwargs))

    def __getattr__(self, name):
        """Fail with the name of the method that this stand-in is missing."""
        raise AttributeError(
            f"{type(self).__name__} does not implement '{name}'. It stands in for "
            "a process pool when there is at most one task, so anything a caller "
            "uses has to be implemented here too. Add it to "
            f"{__name__}.{type(self).__name__}."
        )


def init_worker(log_level: int):
    """Re-establish parent process state that spawned workers don't inherit.

    Forked workers inherited the impersonation wrapper `bqetl` installs over
    `google.auth.default`; spawned workers import `google.auth` fresh and would
    otherwise fall back to the caller's own ADC.
    """
    # Imported here so `bigquery_etl.util.common` isn't pulled in by callers
    # that only need the pool.
    from bigquery_etl.util.common import IMPERSONATE_ENV_VAR, enable_impersonation

    service_account = os.environ.get(IMPERSONATE_ENV_VAR)
    if service_account:
        enable_impersonation(service_account)
    logging.root.setLevel(log_level)


@contextmanager
def process_pool(parallelism: int, task_count: int):
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

    pathos caches its pools in a module global keyed by an id. Each call here
    gets a unique id and drops the pool on the way out, so pools are never
    shared between call sites: a shared pool would run tasks on workers that
    were created before the start method was set, or with a different
    initializer.
    """
    if sys.platform == "darwin":
        mp.set_start_method("spawn", force=True)
        multiprocessing.set_start_method("spawn", force=True)

    nodes = min(parallelism, task_count)
    if nodes <= 1:
        # A single task isn't worth the few seconds a spawned worker spends
        # re-importing the CLI. Staying in-process also means `-p 1` reports
        # failures with a normal traceback for debugging.
        yield _SerialPool()
        return

    pool = ProcessingPool(
        nodes,
        id=uuid4().hex,
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
