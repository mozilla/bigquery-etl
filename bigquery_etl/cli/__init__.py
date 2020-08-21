"""bigquery-etl CLI."""

import click

from ..cli.query import query
from ..cli.dag import dag
from .._version import __version__


def cli():
    """Create the bigquery-etl CLI."""
    commands = {"query": query, "dag": dag}

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    def group():
        """CLI tools for working with bigquery-etl."""
        pass

    group()


if __name__ == "__main__":
    cli()
