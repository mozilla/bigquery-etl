"""Run a query script's entrypoint in its own process, for `bqetl query backfill`.

Usage: python -m bigquery_etl.backfill.run_script <query.py> <entrypoint> <log_level> [args...]

Entrypoints that read the process-global sys.argv (e.g. argparse) race when
backfill dates run on parallel threads, so each date gets its own interpreter.
"""

import importlib.util
import sys

from bigquery_etl.util.process_pool import init_worker


def run_script(path: str, entrypoint: str, log_level: int, args: list[str]) -> None:
    """Load the script at `path` and call `entrypoint` with `args` as sys.argv."""
    # Restore the parent's impersonation and log level, which a new process lacks.
    init_worker(log_level)
    spec = importlib.util.spec_from_file_location("query", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load query script {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.argv = ["query.py", *args]
    getattr(module, entrypoint)()


if __name__ == "__main__":
    run_script(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:])
