"""bigquery-etl CLI."""

import warnings

import click

from .._version import __version__

# We rename the import, otherwise it affects monkeypatching in tests
from ..cli.alchemer import alchemer as alchemer_
from ..cli.dag import dag
from ..cli.dryrun import dryrun
from ..cli.format import format
from ..cli.glean_usage import glean_usage
from ..cli.query import query
from ..cli.routine import mozfun, routine
from ..cli.view import view
from ..dependency import dependency
from ..glam.cli import glam
from ..operational_monitoring import operational_monitoring
from ..stripe import stripe_
from ..subplat.apple import apple


def cli(prog_name=None):
    """Create the bigquery-etl CLI."""
    commands = {
        "query": query,
        "dag": dag,
        "dependency": dependency,
        "dryrun": dryrun,
        "format": format,
        "routine": routine,
        "mozfun": mozfun,
        "stripe": stripe_,
        "glam": glam,
        "glean_usage": glean_usage,
        "view": view,
        "alchemer": alchemer_,
        "opmon": operational_monitoring,
        "apple": apple,
    }

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    def group():
        """CLI tools for working with bigquery-etl."""
        pass

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )

    group(prog_name=prog_name)


if __name__ == "__main__":
    cli()
