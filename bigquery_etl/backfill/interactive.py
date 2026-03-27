"""Interactive prompting for backfill create command."""

from datetime import date
from pathlib import Path

import rich_click as click

from ..cli.utils import QualifiedTableNameType
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from .utils import qualified_table_name_matching

DATE_TYPE = click.DateTime(formats=["%Y-%m-%d"])


def is_interactive(qualified_table_name, start_date) -> bool:
    """Return True if the command should enter interactive prompting mode."""
    return qualified_table_name is None or start_date is None


def prompt_for_options(sql_dir, qualified_table_name=None) -> dict:
    """Prompt for all options interactively."""
    result = {}

    if qualified_table_name is None:
        dataset_table = click.prompt(
            "Qualified table name (dataset.table)",
            type=QualifiedTableNameType(with_project=False),
        )
        qualified_table_name = f"moz-fx-data-shared-prod.{dataset_table}"
    result["qualified_table_name"] = qualified_table_name

    # Detect query.py and load metadata
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_dir = Path(sql_dir) / project / dataset / table
    is_python_script = (table_dir / "query.py").exists()
    metadata_file = table_dir / METADATA_FILE
    metadata = Metadata.from_file(metadata_file) if metadata_file.exists() else None

    result["start_date"] = click.prompt(
        "Start date - first date to backfill (YYYY-MM-DD)", type=DATE_TYPE
    )

    result["end_date"] = click.prompt(
        "End date - last date to backfill (YYYY-MM-DD)",
        default=str(date.today()),
        type=DATE_TYPE,
    )

    excluded = []
    if click.confirm("Exclude any dates from the backfill?", default=False):
        while True:
            excluded.append(
                click.prompt("Date to exclude (YYYY-MM-DD)", type=DATE_TYPE)
            )
            if not click.confirm("Exclude another date?", default=False):
                break
    result["exclude"] = tuple(excluded)

    watchers = []
    while True:
        watcher = click.prompt("Watcher email address")
        watchers.append(watcher)
        if not click.confirm("Add another watcher?", default=False):
            break
    result["watcher"] = tuple(watchers)

    result["reason"] = click.prompt(
        "Reason for the backfill (include links to any related bugs or tickets)"
    )

    val = click.prompt(
        "Custom query path (leave blank for default)",
        default="",
        show_default=False,
    )
    result["custom_query_path"] = val if val else None

    if is_python_script:
        result["query_script_entrypoint"] = click.prompt(
            "Query script entrypoint (name of the main function in query.py, e.g. 'main')"
        )

        result["query_script_date_arg"] = click.prompt(
            "Query script date argument name (e.g. 'submission-date')"
        )

        val = click.prompt(
            'Query script dry run argument (e.g. "--dry-run", leave blank to skip dry runs)',
            default="",
            show_default=False,
        )
        result["query_script_dry_run_arg"] = val if val else None

        args = []
        if click.confirm("Add other query script arguments?", default=False):
            while True:
                args.append(
                    click.prompt('Query script argument (e.g. "--project=abc")')
                )
                if not click.confirm("Add another argument?", default=False):
                    break
        result["query_script_arg"] = tuple(args)

    result["shredder_mitigation"] = click.confirm(
        "Use shredder mitigation?", default=False
    )

    result["override_retention_range_limit"] = click.confirm(
        "Override retention range limit?", default=False
    )

    depends_on_past = metadata is not None and metadata.scheduling.get(
        "depends_on_past", False
    )
    if depends_on_past:
        result["override_depends_on_past_end_date"] = click.confirm(
            "Override depends-on-past end date check?", default=False
        )

    # ensure options that might not be prompted are set, to satisfy test for completeness
    unprompted_or_conditional = [
        "billing_project",
        "query_script_entrypoint",
        "query_script_date_arg",
        "query_script_arg",
        "query_script_dry_run_arg",
        "override_depends_on_past_end_date",
    ]
    for option in unprompted_or_conditional:
        result[option] = result.get(option)

    return result
