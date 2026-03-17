"""Shared Click options for backfill commands."""

from datetime import date

import click


def start_date():
    """Return a Click option for the backfill start date."""
    return click.option(
        "--start_date",
        "--start-date",
        "-s",
        help="First date to be backfilled. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        required=True,
    )


def end_date():
    """Return a Click option for the backfill end date, defaulting to today."""
    return click.option(
        "--end_date",
        "--end-date",
        "-e",
        help="Last date to be backfilled. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today()),
    )


def exclude():
    """Return a Click option for dates to exclude from the backfill."""
    return click.option(
        "--exclude",
        "-x",
        multiple=True,
        help="Dates excluded from backfill. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=None,
    )


def custom_query_path():
    """Return a Click option for an optional custom query path to use in the backfill."""
    return click.option(
        "--custom_query_path",
        "--custom-query-path",
        help="Path of the custom query to run the backfill. Optional.",
        default=None,
    )


def override_retention():
    """Return a Click flag option to allow backfilling outside the retention policy limit."""
    return click.option(
        "--override-retention-range-limit",
        "--override_retention_range_limit",
        required=False,
        type=bool,
        is_flag=True,
        help="True to allow running a backfill outside the retention policy limit.",
        default=False,
    )


def query_script_entrypoint():
    """Return a Click option for the entrypoint command name in a query.py script."""
    return click.option(
        "--query-script-entrypoint",
        help="Name of the Click command in the query.py to use in the backfill. "
        "Either a @click.command() or a python function, e.g. `--query-script-entrypoint=main`. "
        "Required when custom_query_path is a .py file.",
    )


def query_script_date_arg():
    """Return a Click option for the date argument name accepted by a query.py script."""
    return click.option(
        "--query-script-date-arg",
        help="Name of the date argument of the query.py accepting a YYYY-MM-DD string. "
        "e.g. --query-script-date-arg=submission-date if the script takes a --submission-date arg."
        "Required when custom_query_path is a .py file.",
    )


def query_script_arg():
    """Return a Click option for additional CLI arguments to pass into a query.py script."""
    return click.option(
        "--query-script-arg",
        help="CLI arguments to pass into query.py if backfilling a python script. "
        'Specified like `--query-script-arg="--project=abc"`. '
        "Use this to set a destination table for the script to write to a staging table if needed.",
        multiple=True,
    )
