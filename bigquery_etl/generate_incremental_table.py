"""Run a query with a series of @submission_date values."""

import os.path
import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool


def fromisoformat(string):
    """Construct a date from the output of date.isoformat()."""
    return datetime.strptime(string, "%Y-%m-%d").date()


def path(string):
    """Path to a file that must exist."""
    if not os.path.exists(string):
        raise ValueError()
    return string


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--destination_table",
    required=True,
    help="The destination table flag for `bq query` to which"
    " '$%%Y%%m%%d' will be added",
)
parser.add_argument(
    "--start",
    type=fromisoformat,
    help="The first date for which the query will be submitted,"
    " defaults to today in utc",
)
parser.add_argument(
    "--end",
    type=fromisoformat,
    help="The last date for which the query will be submitted,"
    " defaults to today in utc",
)
parser.add_argument(
    "--max_procs",
    "-P",
    type=int,
    default=1,
    help="Maximum number of queries which may be submitted at once",
)
parser.add_argument(
    "query_options",
    nargs="*",
    help="Arguments to pass directly to `script/entrypoint query`",
)
parser.add_argument(
    "query_file", type=path, help="Path to the query file for `script/entrypoint query`"
)


def _date_range(start=None, end=None, step=None):
    """Generate dates from START to END, in steps of STEP days."""
    if step is None:
        if end is not None and start is not None and end < start:
            step = -1
        else:
            step = 1
    if isinstance(step, int):
        step = timedelta(days=step)
    if end is None:
        end = datetime.utcnow().date()
    if start is None:
        start = end - step
    for offset in range((end - start) // step):
        yield start + (offset * step)


def _query(day, entrypoint, destination_table, query_options, query_file):
    proc = subprocess.Popen(
        [
            entrypoint,
            "query",
            "--replace",
            "--dataset_id=telemetry",
            "--destination_table=" + destination_table + day.strftime("$%Y%m%d"),
            "--parameter=submission_date:DATE:" + day.isoformat(),
            "--max_rows=0",
            "--nouse_legacy_sql",
            "--quiet",
        ]
        + query_options
        + [query_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    prefix = day.isoformat() + ": "
    sys.stderr.write(prefix + "processing\n")
    for data, dest in zip(proc.communicate(), (sys.stdout, sys.stderr)):
        lines = data.split(b"\n")
        if len(lines[-1]) == 0:
            lines = lines[:-1]
        for line in lines:
            dest.write(prefix)
            dest.write(line.decode())
            dest.write("\n")
        dest.flush()
    return proc.returncode


def main():
    """Run a query with a series of @submission_date values."""
    args, unknown_args = parser.parse_known_args()
    # Unknown args should be passed as query_options.
    args.query_options.extend(unknown_args)
    target = partial(
        _query,
        entrypoint=os.path.join(os.path.dirname(sys.argv[0]), "entrypoint"),
        destination_table=args.destination_table,
        query_options=args.query_options,
        query_file=args.query_file,
    )
    if args.max_procs == 1:
        for day in _date_range(args.start, args.end):
            target(day)
    else:
        Pool(processes=args.max_procs).map(
            target, _date_range(args.start, args.end), chunksize=1
        )


if __name__ == "__main__":
    main()
