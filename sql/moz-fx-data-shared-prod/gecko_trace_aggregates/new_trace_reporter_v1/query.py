#!/usr/bin/env python3
"""Weekly Bugzilla reporter for new gecko trace patterns.

The Bugzilla API key must be set in the BUGZILLA_API_KEY environment variable.
In Airflow, set this as a secret environment variable on the task.
"""

import datetime
import logging
import os
import sys

import click

from bigquery_etl.gecko_trace.bugzilla_reporter import BugzillaReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@click.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Reference date for the 7-day window (default: today).",
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
    """File Bugzilla bugs for new gecko trace patterns."""
    api_key = os.environ.get("BUGZILLA_API_KEY")
    if not api_key:
        click.echo(
            "Error: set the BUGZILLA_API_KEY environment variable.", err=True
        )
        sys.exit(1)

    reference_date = date.date() if date else None

    reporter = BugzillaReporter(api_key=api_key, project=project, dev=dev)
    reporter.report_new_traces(reference_date=reference_date)


if __name__ == "__main__":
    main()
