"""bigquery-etl CLI."""

import click

from bigquery_etl.cli.query import query_cli
from bigquery_etl.version import __version__


def cli():
    """Create the bigquery-etl CLI."""
    commands = {
        "query": query_cli,
    }

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    def group():
        """CLI tools for working with bigquery-etl."""

    group()


if __name__ == "__main__":
    cli()
