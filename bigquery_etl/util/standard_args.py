"""Standard definitions for reusable script arguments."""

from argparse import Action
from functools import partial
import fnmatch
import re
import logging

from google.cloud import bigquery


def add_argument(parser, *args, **kwargs):
    """Add default to help while adding argument to parser."""
    if "help" in kwargs:
        default = kwargs.get("default")
        if default not in (None, [], [None]):
            if kwargs.get("nargs") in ("*", "+"):
                # unnest a single default for printing, if possible
                try:
                    (default,) = default
                except ValueError:
                    pass
            kwargs["help"] += f"; Defaults to {default}"
    parser.add_argument(*args, **kwargs)


def add_billing_projects(parser, *extra_args, default=[None]):
    """Add argumnt for billing projects."""
    add_argument(
        parser,
        "-p",
        "--billing-projects",
        "--billing_projects",
        "--billing-project",
        "--billing_project",
        *extra_args,
        nargs="+",
        default=default,
        help="One or more billing projects over which bigquery jobs should be "
        "distributed",
    )


def add_dry_run(parser, debug_log_queries=True):
    """Add argument for dry run."""
    add_argument(
        parser,
        "--dry_run",
        "--dry-run",
        action="store_true",
        help="Do not make changes, only log actions that would be taken"
        + (
            "; Use with --log-level=DEBUG to log query contents"
            if debug_log_queries
            else ""
        ),
    )


def add_log_level(parser, default=logging.getLevelName(logging.INFO)):
    """Add argument for log level."""
    add_argument(
        parser,
        "-l",
        "--log-level",
        "--log_level",
        action=LogLevelAction,
        default=default,
        type=str.upper,
        help="Set logging level for the python root logger",
    )


def add_parallelism(parser, default=4):
    """Add argument for parallel execution."""
    add_argument(
        parser,
        "-P",
        "--parallelism",
        default=default,
        type=int,
        help="Maximum number of tasks to execute concurrently",
    )


def add_priority(parser):
    """Add argument for BigQuery job priority."""
    add_argument(
        parser,
        "--priority",
        default=bigquery.QueryPriority.INTERACTIVE,
        type=str.upper,
        choices=[bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
        help="Priority for BigQuery query jobs; BATCH priority may significantly slow "
        "down queries if reserved slots are not enabled for the billing project; "
        "INTERACTIVE priority is limited to 100 concurrent queries per project",
    )


def add_table_filter(parser, example="telemetry_stable.main_v*"):
    """Add arguments for filtering tables."""
    example_ = f"Pass names or globs like {example!r}"
    add_argument(
        parser,
        "-o",
        "--only",
        nargs="+",
        dest="table_filter",
        raw_dest="only_tables",
        action=TableFilterAction,
        help=f"Process only the given tables; {example_}",
    )
    add_argument(
        parser,
        "-x",
        "--except",
        nargs="+",
        dest="table_filter",
        raw_dest="except_tables",
        action=TableFilterAction,
        help=f"Process all tables except for the given tables; {example_}",
    )


class LogLevelAction(Action):
    """Custom argparse.Action for --log-level."""

    def __init__(self, *args, **kwargs):
        """Set default log level if provided."""
        super().__init__(*args, **kwargs)
        if self.default is not None:
            logging.root.setLevel(self.default)

    def __call__(self, parser, namespace, value, option_string=None):
        """Set level for root logger."""
        logging.root.setLevel(value)


class TableFilterAction(Action):
    """Custom argparse.Action for --only and --except."""

    def __init__(self, *args, raw_dest, **kwargs):
        """Add default."""
        super().__init__(*args, default=self.default, **kwargs)
        self.raw_dest = raw_dest
        self.arg = self.option_strings[-1]
        self.invert_match = self.arg == "--except"

    @staticmethod
    def default(table):
        """Return True for default predicate."""
        return True

    @staticmethod
    def compile(values):
        """Compile a list of glob patterns into a single regex."""
        return re.compile("|".join(fnmatch.translate(pattern) for pattern in values))

    def predicate(self, table, pattern):
        """Log tables skipped due to table filter arguments."""
        matched = (pattern.match(table) is not None) != self.invert_match
        if not matched:
            logging.info(f"Skipping {table} due to {self.arg} argument")
        return matched

    def __call__(self, parser, namespace, values, option_string=None):
        """Add table filter to predicates."""
        setattr(namespace, self.raw_dest, values)
        predicates_attr = "_" + self.dest
        predicates = getattr(namespace, predicates_attr, [])
        if not hasattr(namespace, predicates_attr):
            setattr(namespace, predicates_attr, predicates)
            setattr(
                namespace,
                self.dest,
                lambda table: all(predicate(table) for predicate in predicates),
            )
        predicates.append(partial(self.predicate, pattern=self.compile(values)))
