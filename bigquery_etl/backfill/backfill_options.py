"""Shared Click options for backfill commands."""

from datetime import date

import click


def start_date():
    return click.option(
        "--start_date",
        "--start-date",
        "-s",
        help="First date to be backfilled. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        required=True,
    )


def end_date():
    return click.option(
        "--end_date",
        "--end-date",
        "-e",
        help="Last date to be backfilled. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=str(date.today()),
    )


def exclude():
    return click.option(
        "--exclude",
        "-x",
        multiple=True,
        help="Dates excluded from backfill. Date format: yyyy-mm-dd",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=None,
    )


def custom_query_path():
    return click.option(
        "--custom_query_path",
        "--custom-query-path",
        help="Path of the custom query to run the backfill. Optional.",
        default=None,
    )


def override_retention():
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
    return click.option(
        "--query-script-entrypoint",
        help="Name of the Click command in the query.py to use in the backfill. "
        "Either a @click.command() or a python function, e.g. `--query-script-entrypoint=main`. "
        "Required when custom_query_path is a .py file.",
    )


def query_script_date_arg():
    return click.option(
        "--query-script-date-arg",
        help="Name of the date argument of the query.py accepting a YYYY-MM-DD string. "
        "e.g. --query-script-date-arg=submission-date if the script takes a --submission-date arg."
        "Required when custom_query_path is a .py file.",
    )


def query_script_arg():
    return click.option(
        "--query-script-arg",
        help="CLI arguments to pass into query.py if backfilling a python script. "
        'Specified like `--query-script-arg="--project=abc"`. '
        "Use this to set a destination table for the script to write to a staging table if needed.",
        multiple=True,
    )
