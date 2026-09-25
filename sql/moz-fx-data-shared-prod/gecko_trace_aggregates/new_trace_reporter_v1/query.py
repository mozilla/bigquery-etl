#!/usr/bin/env python3
"""Weekly Bugzilla reporter for gecko trace patterns.

Files one bug per new trace pattern and posts a weekly update to the bugs of
trace patterns that are still seen. The Bugzilla API key comes from the
BUGZILLA_API_KEY environment variable.
"""

import datetime
import logging
import os
import sys

import click

from bigquery_etl.gecko_trace.bugzilla_reporter import BugzillaClient, BugzillaReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@click.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Last day of the 7-day window (default: today).",
)
@click.option(
    "--project",
    default="moz-fx-data-shared-prod",
    help="BigQuery project.",
)
@click.option(
    "--dev",
    is_flag=True,
    default=False,
    help="Use the Bugzilla dev instance (bugzilla-dev.allizom.org).",
)
def main(date, project, dev):
    """File and update Bugzilla bugs for gecko trace patterns."""
    api_key = os.environ.get("BUGZILLA_API_KEY")
    if not api_key:
        click.echo("Error: set the BUGZILLA_API_KEY environment variable.", err=True)
        sys.exit(1)

    reference_date = date.date() if date else datetime.date.today()
    reporter = BugzillaReporter(
        BugzillaClient(api_key=api_key, dev=dev), project=project
    )
    reporter.report(reference_date=reference_date)


if __name__ == "__main__":
    main()
