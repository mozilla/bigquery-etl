"""bigquery-etl CLI data_governance command."""

from datetime import datetime, timezone
from pathlib import Path

import rich_click as click

from ..cli.utils import sql_dir_option
from ..util.common import block_coding_agents
from .classification import runner, upstream
from .classification.config import (
    DEFAULT_DATASET,
    DEFAULT_MODEL,
    DEFAULT_PROJECT,
    DEFAULT_VERTEX_LOCATION,
    ClassificationConfig,
    validate_bq_identifier,
)
from .classification.runner import TargetKey


def _parse_target(value: str) -> TargetKey:
    """Turn `project.dataset[.table]` into the triple the library takes."""
    parts = value.split(".")
    if len(parts) not in (2, 3):
        message = (
            f"Invalid classification target {value!r}. "
            "Expected project.dataset or project.dataset.table."
        )
        if len(parts) == 1:
            message += " The project is required."
        raise click.BadParameter(message)
    try:
        project = validate_bq_identifier(parts[0], "target project")
        dataset = validate_bq_identifier(parts[1], "target dataset")
        table = (
            validate_bq_identifier(parts[2], "target table")
            if len(parts) == 3
            else None
        )
    except ValueError as e:
        raise click.BadParameter(str(e))
    return project, dataset, table


def _parse_targets(ctx, param, value) -> list[TargetKey]:
    """Parse the positional targets, keeping the order they were given in."""
    return [_parse_target(target) for target in value]


# Positional so that it cannot be confused with the global `bqetl --target`,
# which selects a deployment environment. Each value carries its project, since
# the tables being classified need not live where the classification tables do.
targets_argument = click.argument(
    "targets", nargs=-1, required=True, callback=_parse_targets
)

# Required rather than defaulted, because the date is what joins the three
# upstream steps: each reads the newest partition of the step before it. Three
# defaults evaluated from three clocks would split a pass that crosses midnight.
date_option = click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Partition the step reads and writes, as YYYY-MM-DD.",
)

project_option = click.option(
    "--project",
    default=DEFAULT_PROJECT,
    help="Project holding the classification tables.",
)

dataset_option = click.option(
    "--dataset",
    default=DEFAULT_DATASET,
    help="Dataset holding the classification tables.",
)

run_id_option = click.option(
    "--run-id",
    "--run_id",
    default=None,
    help="Identifier stamped on the rows this run writes. "
    "Defaults to a UTC timestamp.",
)

max_workers_option = click.option(
    "--max-workers",
    "--max_workers",
    type=int,
    default=None,
    help="Threads the upstream script uses. Defaults to the script's own value.",
)

dry_run_option = click.option(
    "--dry-run",
    "--dry_run",
    is_flag=True,
    default=False,
    help="Ask the upstream script what it would do, without writing anything.",
)


def _config(
    run_id: str | None, project: str, dataset: str, **fields
) -> ClassificationConfig:
    """Build the run's config, generating a run id when one was not given.

    The id is generated here rather than read from the global `bqetl --run-id`,
    which exists to render target templates. It is written to every output row,
    so overloading a flag documented for something else would be misleading.
    """
    try:
        return ClassificationConfig(
            run_id=run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
            project=project,
            dataset=dataset,
            **fields,
        )
    except ValueError as e:
        # These checks all cover a value that arrived from a flag, so naming the
        # flag beats printing a traceback out of the dataclass.
        raise click.BadParameter(str(e))


@click.group(help="""Tools for data governance metadata.

    Examples:

    \b
    # Fill the classifier's input tables for one date, then classify a dataset.
    $ ./bqetl data_governance profile --date=2026-08-20 \\
        moz-fx-data-shared-prod.telemetry_derived
    $ ./bqetl data_governance lineage --date=2026-08-20
    $ ./bqetl data_governance probes --date=2026-08-20
    $ ./bqetl data_governance classify moz-fx-data-shared-prod.telemetry_derived
    """)
def data_governance():
    """Create the CLI group for the data_governance command."""


@data_governance.command()
@block_coding_agents
@targets_argument
@date_option
@project_option
@dataset_option
@run_id_option
@sql_dir_option
@click.option(
    "--temp-dataset",
    "--temp_dataset",
    default=None,
    help="Dataset the scratch profile tables are created in, as PROJECT.DATASET. "
    "Defaults to `tmp` in --project.",
)
@max_workers_option
@dry_run_option
def profile(
    targets,
    date,
    project,
    dataset,
    run_id,
    sql_dir,
    temp_dataset,
    max_workers,
    dry_run,
):
    """Profile the given classification targets into the profiles table.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.
    """
    upstream.profile(
        _config(run_id, project, dataset),
        targets,
        date.date(),
        sql_dir=Path(sql_dir),
        temp_dataset=temp_dataset,
        max_workers=max_workers,
        dry_run=dry_run,
    )


@data_governance.command()
@block_coding_agents
@date_option
@project_option
@dataset_option
@run_id_option
@sql_dir_option
@max_workers_option
@dry_run_option
def lineage(date, project, dataset, run_id, sql_dir, max_workers, dry_run):
    """Resolve each profiled table's ping into the lineage table.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.
    """
    upstream.resolve_lineage(
        _config(run_id, project, dataset),
        date.date(),
        sql_dir=Path(sql_dir),
        max_workers=max_workers,
        dry_run=dry_run,
    )


@data_governance.command()
@block_coding_agents
@date_option
@project_option
@dataset_option
@run_id_option
@sql_dir_option
@max_workers_option
@dry_run_option
def probes(date, project, dataset, run_id, sql_dir, max_workers, dry_run):
    """Fetch the probe definitions of every ping in the lineage table.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.
    """
    upstream.fetch_probes(
        _config(run_id, project, dataset),
        date.date(),
        sql_dir=Path(sql_dir),
        max_workers=max_workers,
        dry_run=dry_run,
    )


@data_governance.command()
@block_coding_agents
@targets_argument
@project_option
@dataset_option
@run_id_option
@click.option(
    "--model",
    default=DEFAULT_MODEL,
    help="Vertex-hosted Gemini model the columns are classified with.",
)
@click.option(
    "--vertex-location",
    "--vertex_location",
    default=DEFAULT_VERTEX_LOCATION,
    help="Vertex AI location the model is called in.",
)
@click.option(
    "--vertex-project",
    "--vertex_project",
    default=None,
    help="Project the model is called in. Defaults to --project.",
)
@click.option(
    "--dlp-project",
    "--dlp_project",
    default=None,
    help="Project the DLP inspection runs in. Defaults to --project.",
)
@click.option(
    "--dlp-quota-project",
    "--dlp_quota_project",
    default=None,
    help="Project DLP quota is billed to. Defaults to the credentials' own.",
)
@click.option(
    "--refresh",
    is_flag=True,
    default=False,
    help="Classify columns an earlier run already did, instead of skipping them.",
)
def classify(
    targets,
    project,
    dataset,
    run_id,
    model,
    vertex_location,
    vertex_project,
    dlp_project,
    dlp_quota_project,
    refresh,
):
    """Classify the columns of the given classification targets.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.
    """
    runner.run(
        _config(
            run_id,
            project,
            dataset,
            model=model,
            vertex_location=vertex_location,
            vertex_project=vertex_project,
            dlp_project=dlp_project,
            dlp_quota_project=dlp_quota_project,
        ),
        targets,
        refresh=refresh,
    )
