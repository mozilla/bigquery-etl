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
from ..util.common import enable_impersonation, set_resolved_target_project
from ..util.target import (
    get_default_target_name,
    get_target,
    set_grant_impersonation_access,
)


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
        "--no-impersonate",
        "--no_impersonate",
        is_flag=True,
        default=False,
        help="Run with your own credentials instead of impersonating the target's "
        "service account. Not available to coding agents for write/deploy/backfill.",
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
    def group(ctx, log_level, target, no_target, no_impersonate, run_id):
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

        # --no-impersonate opts out for this process, even if the env var was
        # exported externally. The agent gate refuses write/deploy/backfill
        # without impersonation.
        env_var = "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"
        if no_impersonate:
            os.environ.pop(env_var, None)

        try:
            if not target and not no_target:
                target = get_default_target_name()

            parsed_target = None
            if target:
                parsed_target = get_target(target, run_id=run_id)
                ctx.obj["target"] = parsed_target
                # Expose the target project to the coding-agent gate.
                set_resolved_target_project(parsed_target.project_id)
                # And the target's dataset-access preference for impersonation.
                set_grant_impersonation_access(parsed_target.grant_impersonation_access)
                click.echo(
                    f"ℹ️  Using target: {parsed_target.name} (project: {parsed_target.project_id})"
                )

            # Impersonate the target's SA (explicit env var wins): env var for
            # `bq`/`gcloud` shell-outs, enable_impersonation for Python clients.
            if not no_impersonate:
                sa = os.environ.get(env_var) or (
                    parsed_target.impersonate_service_account if parsed_target else None
                )
                if sa:
                    os.environ[env_var] = sa
                    enable_impersonation(sa)
                    click.echo(f"ℹ️  Impersonating service account: {sa}")
        except Exception as e:
            raise click.ClickException(f"Failed to load target '{target}': {e}")

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )

    group(prog_name=prog_name)


if __name__ == "__main__":
    ConfigLoader.set_project_dir(Path().absolute())
    cli()
