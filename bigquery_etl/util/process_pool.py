"""Process pool creation for SQL generators."""

import sys
from contextlib import contextmanager

from pathos.helpers import mp
from pathos.multiprocessing import ProcessingPool


@contextmanager
def process_pool(parallelism, pool_id):
    """Create a pathos process pool with `parallelism` workers.

    On macOS, workers that make an HTTP request after the parent process has
    made one segfault in the fork-unsafe parts of CoreFoundation (DNS
    resolution, proxy lookup), and the pool then waits forever on the task the
    dead worker was holding.
    See https://github.com/mozilla/bigquery-etl/issues/9821 and
    https://github.com/python/cpython/issues/75999. Spawn avoids this, at the
    cost of slower worker startup and cold caches in the workers.

    Callers must be import-safe: with spawn the workers re-import the entry
    point's `__main__`, so a script that creates the pool at module level
    instead of under `if __name__ == "__main__":` will deadlock.

    Pools are cached by `pool_id`, so give each call site its own id. Reusing
    another generator's pool would run the tasks on workers that were created
    before the start method was set.
    """
    if sys.platform == "darwin":
        mp.set_start_method("spawn", force=True)
    pool = ProcessingPool(parallelism, id=pool_id)
    try:
        yield pool
    except BaseException:
        pool.terminate()
        raise
    finally:
        # pathos caches pools and doesn't release them when the context exits
        pool.clear()
