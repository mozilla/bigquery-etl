"""bigquery-etl CLI."""

import logging
import os
import warnings
from pathlib import Path

import rich_click as click

from .._version import __version__

# We rename the import, otherwise it affects monkeypatching in tests
from ..cli.alchemer import alchemer as alchemer_
from ..cli.backfill import backfill
from ..cli.check import check
from ..cli.dag import dag
from ..cli.deploy import deploy
from ..cli.dryrun import dryrun
from ..cli.format import format
from ..cli.generate import generate
from ..cli.metadata import metadata
from ..cli.monitoring import monitoring
from ..cli.query import query
from ..cli.routine import mozfun, routine
from ..cli.stage import stage
from ..cli.static import static_
from ..cli.target import target
from ..cli.view import view
from ..config import ConfigLoader
from ..copy_deduplicate import copy_deduplicate
from ..dependency import dependency
from ..docs import docs_
from ..glam.cli import glam
from ..stripe import stripe_
from ..subplat.apple import apple
from ..util.common import set_resolved_target_project
from ..util.target import get_default_target_name, get_target


def cli(prog_name=None):
    """Create the bigquery-etl CLI."""
    commands = {
        "query": query,
        "dag": dag,
        "dependency": dependency,
        "deploy": deploy,
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
        "monitoring": monitoring,
        "target": target,
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
    @click.option(
        "--target",
        help="Target environment to use for commands that interact with BigQuery. See"
        " `bqetl_targets.yaml` for available targets. Overrides the BQETL_TARGET"
        " environment variable and the default_target setting in bqetl_targets.yaml.",
    )
    @click.option(
        "--no-target",
        "--no_target",
        is_flag=True,
        default=False,
        help="Disable the default target, ignoring BQETL_TARGET and default_target in bqetl_targets.yaml.",
    )
    @click.option(
        "--run-id",
        "--run_id",
        "run_id",
        default=None,
        help="Per-invocation run id rendered into target templates as `{{ run_id }}`. "
        "Used to disambiguate parallel deploys for the same git.branch/git.commit "
        "(e.g. concurrent CI runs). Defaults to $BQETL_RUN_ID if set.",
    )
    @click.pass_context
    def group(ctx, log_level, target, no_target, run_id):
        """CLI tools for working with bigquery-etl."""
        logging.root.setLevel(level=log_level)

        # Only fall back to BQETL_RUN_ID (opt-in) — not GITHUB_RUN_ID, which
        # GitHub Actions auto-sets on every runner. A local invocation from a
        # CI-adjacent shell would otherwise silently inherit an unrelated run
        # id and filter / render against the wrong dataset.
        if run_id is None:
            run_id = os.environ.get("BQETL_RUN_ID") or None

        ctx.ensure_object(dict)
        ctx.obj["target"] = None
        ctx.obj["run_id"] = run_id

        try:
            if not target and not no_target:
                target = get_default_target_name()

            if target:
                parsed_target = get_target(target, run_id=run_id)
                ctx.obj["target"] = parsed_target
                # Let the coding-agent gate see which project this invocation
                # is scoped to, so agents may run against dev/sandbox targets.
                set_resolved_target_project(parsed_target.project_id)
                click.echo(
                    f"ℹ️  Using target: {parsed_target.name} (project: {parsed_target.project_id})"
                )
        except Exception as e:
            raise click.ClickException(f"Failed to load target '{target}': {e}")

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )

    group(prog_name=prog_name)


if __name__ == "__main__":
    ConfigLoader.set_project_dir(Path().absolute())
    cli()
