#!/usr/bin/env python3
"""Daily mapper that points new gecko trace signatures at existing stable ids.

Uses Searchfox blame data to find the same source line across Firefox
releases. The parsed Searchfox responses are cached in BigQuery so that each
(revision, file) is fetched once.
"""

import datetime
import logging

import click

from bigquery_etl.gecko_trace.event_mapper import EventMapper
from bigquery_etl.gecko_trace.searchfox import SEARCHFOX_BASE_URL, SearchfoxClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@click.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Date recorded on the cache rows written by this run (default: today).",
)
@click.option(
    "--project",
    default="moz-fx-data-shared-prod",
    help="BigQuery project.",
)
@click.option(
    "--searchfox-url",
    default=SEARCHFOX_BASE_URL,
    show_default=True,
    help="Searchfox server to query.",
)
def main(date, project, searchfox_url):
    """Update the Searchfox caches and remap event and trace ids."""
    reference_date = date.date() if date else datetime.date.today()
    mapper = EventMapper(SearchfoxClient(base_url=searchfox_url), project=project)
    mapper.run(reference_date=reference_date)


if __name__ == "__main__":
    main()
