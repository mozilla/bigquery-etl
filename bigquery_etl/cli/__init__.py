"""bigquery-etl CLI."""

import logging
import warnings
from pathlib import Path

import click

from .._version import __version__

# We rename the import, otherwise it affects monkeypatching in tests
from ..cli.alchemer import alchemer as alchemer_
from ..cli.backfill import backfill
from ..cli.check import check
from ..cli.dag import dag
from ..cli.dryrun import dryrun
from ..cli.format import format
from ..cli.generate import generate
from ..cli.metadata import metadata
from ..cli.query import query
from ..cli.routine import mozfun, routine
from ..cli.stage import stage
from ..cli.view import view
from ..config import ConfigLoader
from ..copy_deduplicate import copy_deduplicate
from ..dependency import dependency
from ..docs import docs_
from ..glam.cli import glam
from ..static import static_
from ..stripe import stripe_
from ..subplat.apple import apple


def cli(prog_name=None):
    """Create the bigquery-etl CLI."""
    commands = {
        "query": query,
        "dag": dag,
        "dependency": dependency,
        "dryrun": dryrun,
        "generate": generate,
        "format": format,
        "routine": routine,
        "mozfun": mozfun,
        "stripe": stripe_,
        "glam": glam,
        "view": view,
        "alchemer": alchemer_,
        "apple": apple,
        "static": static_,
        "docs": docs_,
        "copy_deduplicate": copy_deduplicate,
        "stage": stage,
        "backfill": backfill,
        "check": check,
        "metadata": metadata,
    }

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    @click.option(
        "--log-level",
        "--log_level",
        help="Log level.",
        default=logging.getLevelName(logging.INFO),
        type=str.upper,
    )
    def group(log_level):
        """CLI tools for working with bigquery-etl."""
        logging.root.setLevel(level=log_level)

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )

    group(prog_name=prog_name)


if __name__ == "__main__":
    ConfigLoader.set_project_dir(Path().absolute())
    cli()
