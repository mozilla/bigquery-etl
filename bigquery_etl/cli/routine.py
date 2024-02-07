"""bigquery-etl CLI UDF command."""

import copy
import os
import re
import shutil
import string
import sys
from fnmatch import fnmatchcase
from glob import glob
from pathlib import Path

import pytest
import rich_click as click
import yaml

from ..cli.format import format
from ..cli.utils import (
    is_authenticated,
    is_valid_project,
    project_id_option,
    sql_dir_option,
)
from ..config import ConfigLoader
from ..docs import validate as validate_docs
from ..format_sql.formatter import reformat
from ..routine import publish_routines
from ..routine.parse_routine import PROCEDURE_FILE, UDF_FILE
from ..util.common import project_dirs

ROUTINE_NAME_RE = re.compile(r"^(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)$")
ROUTINE_DATASET_RE = re.compile(r"^(?P<dataset>[a-zA-z0-9_]+)$")
ROUTINE_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/"
    r"(udf\.sql|stored_procedure\.sql)$"
)


def _routines_matching_name_pattern(pattern, sql_path, project_id):
    """Return paths to routines matching the name pattern."""
    sql_path = Path(sql_path)
    if project_id is not None:
        sql_path = sql_path / project_id

    all_sql_files = map(Path, glob(f"{sql_path}/**/*.sql", recursive=True))
    routine_files = []

    for sql_file in all_sql_files:
        match = ROUTINE_FILE_RE.match(str(sql_file))
        if match:
            project = match.group(1)
            dataset = match.group(2)
            routine_name = match.group(3)
            routine_name = f"{project}.{dataset}.{routine_name}"
            if fnmatchcase(routine_name, f"*{pattern}"):
                routine_files.append(sql_file)
            elif project_id and fnmatchcase(routine_name, f"{project_id}.{pattern}"):
                routine_files.append(sql_file)

    return routine_files


def get_project_id(ctx, project_id=None):
    """Return the project id with the option flag taking priority."""
    if project_id:
        return project_id
    default_project = ctx.obj["DEFAULT_PROJECT"]
    if default_project and is_valid_project(ctx, None, default_project):
        return default_project
    click.echo(
        "Please specify a project_id e.g. --project_id=moz-fx-data-shared-prod",
        err=True,
    )
    sys.exit(1)


@click.group(help="Commands for managing routines for internal use.")
@click.pass_context
def routine(ctx):
    """Create the CLI group for the routine command."""
    ctx.ensure_object(dict)
    ctx.obj["DEFAULT_PROJECT"] = ConfigLoader.get("default", "project")


@click.group(help="Commands for managing public mozfun routines.")
@click.pass_context
def mozfun(ctx):
    """Create the CLI group for the mozfun command."""
    ctx.ensure_object(dict)
    ctx.obj["DEFAULT_PROJECT"] = ConfigLoader.get("routine", "project")


@routine.command(
    help="""Create a new routine. Specify whether the routine is a UDF or
    stored procedure by adding a --udf or --stored_prodecure flag.

    Examples:

    \b
    # Create a UDF
    ./bqetl routine create --udf udf.array_slice

    \b
    # Create a stored procedure
    ./bqetl routine create --stored_procedure udf.events_daily

    \b
    # Create a UDF in a project other than shared-prod
    ./bqetl routine create --udf udf.active_last_week --project=moz-fx-data-marketing-prod
    """
)
@click.argument("name")
@sql_dir_option
@project_id_option()
@click.option("--udf", "-u", is_flag=True, help="Create a new UDF", default=False)
@click.option(
    "--stored_procedure",
    "--stored-procedure",
    "-p",
    is_flag=True,
    help="Create a new stored procedure",
    default=False,
)
@click.pass_context
def create(ctx, name, sql_dir, project_id, udf, stored_procedure):
    """CLI command for creating a new routine."""
    if (udf is False and stored_procedure is False) or (udf and stored_procedure):
        click.echo(
            "Please specify if new routine is a UDF or stored procedure by adding "
            "either a --udf or --stored_prodecure flag: "
            "bqetl routine create <dataset>.<name> --udf/--stored_procedure",
            err=True,
        )
        sys.exit(1)

    project_id = get_project_id(ctx, project_id)

    # create directory structure
    try:
        match = ROUTINE_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")
    except AttributeError:
        click.echo("New routine must be named like: <dataset>.<routine_name>")
        sys.exit(1)

    routine_path = Path(sql_dir) / project_id / dataset / name
    routine_path.mkdir(parents=True)

    assert_udf_qualifier = "" if project_id == "mozfun" else "mozfun."

    # create SQL file with UDF definition
    if udf:
        routine_file = routine_path / UDF_FILE
        routine_file.write_text(
            reformat(
                f"""
                -- Definition for {dataset}.{name}
                -- For more information on writing UDFs see:
                -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
                CREATE OR REPLACE FUNCTION {dataset}.{name}()
                RETURNS BOOLEAN AS (
                    TRUE
                );

                -- Tests
                SELECT {assert_udf_qualifier}assert.true({dataset}.{name}())
                """
            )
            + "\n"
        )
    elif stored_procedure:
        stored_procedure_file = routine_path / PROCEDURE_FILE
        stored_procedure_file.write_text(
            reformat(
                f"""
                -- Definition for {dataset}.{name}
                CREATE OR REPLACE PROCEDURE {dataset}.{name}()
                BEGIN

                END;

                -- Tests
                SELECT {assert_udf_qualifier}assert.true({dataset}.{name}())
                """
            )
            + "\n"
        )

    # create default metadata.yaml
    metadata_file = routine_path / "metadata.yaml"
    metadata = {
        "friendly_name": string.capwords(name.replace("_", " ")),
        "description": "Please provide a description for the routine",
    }
    metadata_file.write_text(yaml.dump(metadata))

    # create stub README.md
    readme_file = routine_path / "README.md"
    readme_file.write_text(
        (
            "\n".join(
                map(
                    lambda l: l.lstrip(),
                    """
            <!--
            This is a short README for your routine, you can add any extra
            documentation or examples that a user might want to see when
            viewing the documentation at https://mozilla.github.io/bigquery-etl

            You can embed an SQL file into your README using the following
            syntax:

            @sql(../examples/fenix_app_info.sql)
            -->
            """.split(
                        "\n"
                    ),
                )
            )
        )
        + "\n"
    )


mozfun.add_command(copy.copy(create))
mozfun.commands[
    "create"
].help = """
Create a new mozfun routine. Specify whether the routine is a UDF or
stored procedure by adding a --udf or --stored_prodecure flag. UDFs
are added to the `mozfun` project.

Examples:

\b
# Create a UDF
./bqetl mozfun create --udf bytes.zero_right

\b
# Create a stored procedure
./bqetl mozfun create --stored_procedure event_analysis.events_daily
"""


@routine.command(
    help="""Get routine information.

    Examples:

    \b
    # Get information about all internal routines in a specific dataset
    ./bqetl routine info udf.*

    \b
    # Get usage information of specific routine
    ./bqetl routine info --usages udf.get_key
    """
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
@click.option("--usages", "-u", is_flag=True, help="Show routine usages", default=False)
@click.pass_context
def info(ctx, name, sql_dir, project_id, usages):
    """CLI command for returning information about routines."""
    project_id = get_project_id(ctx, project_id)

    if name is None:
        name = "*.*"

    routine_files = _routines_matching_name_pattern(name, sql_dir, project_id)

    for routine_file in routine_files:
        routine_file_path = Path(routine_file)
        routine_name = routine_file_path.parent.name
        routine_dataset = routine_file_path.parent.parent.name
        routine_project = routine_file_path.parent.parent.parent.name

        try:
            metadata = yaml.safe_load(open(routine_file_path.parent / "metadata.yaml"))
        except FileNotFoundError:
            metadata = None

        click.secho(f"{routine_project}.{routine_dataset}.{routine_name}", bold=True)
        click.echo(f"path: {routine_file_path}")

        if metadata is None:
            click.echo("No metadata")
        else:
            click.echo(f"description: {metadata['description']}")

        no_usages = True
        if usages:
            # find routine usages in SQL files
            click.echo("usages: ")
            sql_files = [
                p
                for project in project_dirs()
                for p in map(Path, glob(f"{project}/**/*.sql", recursive=True))
            ]
            for sql_file in sql_files:
                if f"{routine_dataset}.{routine_name}" in sql_file.read_text():
                    no_usages = False
                    click.echo(f"  {sql_file}")

            if no_usages:
                click.echo("  No usages.")

        click.echo("")


mozfun.add_command(copy.copy(info))
mozfun.commands[
    "info"
].help = """Get mozfun routine information.

Examples:

\b
# Get information about all internal routines in a specific dataset
./bqetl mozfun info hist.*

\b
# Get usage information of specific routine
./bqetl mozfun info --usages hist.mean
"""


@routine.command(
    help="""Validate formatting of routines and run tests.

    Examples:

    \b
    # Validate all routines
    ./bqetl routine validate

    \b
    # Validate selected routines
    ./bqetl routine validate udf.*
    """,
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
@click.option(
    "--docs-only",
    "--docs_only",
    default=False,
    is_flag=True,
    help="Only validate docs.",
)
@click.pass_context
def validate(ctx, name, sql_dir, project_id, docs_only):
    """Validate routines by formatting and running tests."""
    project_id = get_project_id(ctx, project_id)

    if name is None:
        name = "*.*"

    routine_files = _routines_matching_name_pattern(name, sql_dir, project_id)

    ctx.invoke(validate_docs, project_dirs=project_dirs(project_id))

    if not docs_only:
        for routine_file in routine_files:
            ctx.invoke(format, paths=[str(routine_file.parent)], check=True)
            pytest.main([str(routine_file.parent)])


mozfun.add_command(copy.copy(validate))
mozfun.commands[
    "validate"
].help = """Validate formatting of mozfun routines and run tests.

Examples:

\b
# Validate all routines
./bqetl mozfun validate

\b
# Validate selected routines
./bqetl mozfun validate hist.*
"""


@routine.command(
    help="""Publish routines to BigQuery. Requires service account access.

    Examples:

    \b
    # Publish all routines
    ./bqetl routine publish

    \b
    # Publish selected routines
    ./bqetl routine validate udf.*
    """,
)
@click.argument("name", required=False)
@project_id_option()
@click.option(
    "--dependency-dir",
    "--dependency_dir",
    default=ConfigLoader.get("routine", "dependency_dir"),
    help="The directory JavaScript dependency files for UDFs are stored.",
)
@click.option(
    "--gcs-bucket",
    "--gcs_bucket",
    default=ConfigLoader.get("routine", "publish", "gcs_bucket"),
    help="The GCS bucket where dependency files are uploaded to.",
)
@click.option(
    "--gcs-path",
    "--gcs_path",
    default=ConfigLoader.get("routine", "publish", "gcs_path"),
    help="The GCS path in the bucket where dependency files are uploaded to.",
)
@click.option(
    "--dry_run/--no_dry_run", "--dry-run/--no-dry-run", help="Dry run publishing udfs."
)
@click.pass_context
def publish(ctx, name, project_id, dependency_dir, gcs_bucket, gcs_path, dry_run):
    """Publish routines."""
    project_id = get_project_id(ctx, project_id)

    public = False

    if not is_authenticated():
        click.echo("User needs to be authenticated to publish routines.", err=True)
        sys.exit(1)

    click.echo(f"Publish routines to {project_id}")
    # NOTE: this will only publish to a single project
    for target in project_dirs(project_id):
        publish_routines.publish(
            target,
            project_id,
            dependency_dir,
            gcs_bucket,
            gcs_path,
            public,
            pattern=name,
            dry_run=dry_run,
        )
        click.echo(f"Published routines to {project_id}")


mozfun.add_command(copy.copy(publish))
mozfun.commands[
    "publish"
].help = """Publish mozfun routines. This command is used
by Airflow only."""


@routine.command(
    help="""Rename routine or routine dataset. Replaces all usages in queries with
    the new name.

    Examples:

    \b
    # Rename routine
    ./bqetl routine rename udf.array_slice udf.list_slice

    \b
    # Rename routine matching a specific pattern
    ./bqetl routine rename udf.array_* udf.list_*
    """,
)
@click.argument("name", required=True)
@click.argument("new_name", required=True)
@sql_dir_option
@project_id_option()
@click.pass_context
def rename(ctx, name, new_name, sql_dir, project_id):
    """Rename routines based on pattern."""
    project_id = get_project_id(ctx, project_id)

    routine_files = _routines_matching_name_pattern(name, sql_dir, project_id)

    if ROUTINE_NAME_RE.match(new_name) and len(routine_files) <= 1:
        # rename routines
        match = ROUTINE_NAME_RE.match(new_name)
        new_routine_name = match.group("name")
        new_routine_dataset = match.group("dataset")
    elif ROUTINE_DATASET_RE.match(new_name):
        # rename routines dataset
        match = ROUTINE_DATASET_RE.match(new_name)
        new_routine_name = None
        new_routine_dataset = match.group("dataset")
    else:
        click.echo("Invalid rename naming patterns.")
        sys.exit(1)

    for routine_file in routine_files:
        # move to new directories
        old_full_routine_name = (
            f"{routine_file.parent.parent.name}.{routine_file.parent.name}"
        )

        if new_routine_name and new_routine_dataset:
            source = routine_file.parent
            destination = (
                routine_file.parent.parent.parent
                / new_routine_dataset
                / new_routine_name
            )
            new_full_routine_name = f"{new_routine_dataset}.{new_routine_name}"
        else:
            source = routine_file.parent.parent
            destination = routine_file.parent.parent.parent / new_routine_dataset
            new_full_routine_name = f"{new_routine_dataset}.{routine_file.parent.name}"

        if source.exists():
            os.makedirs(destination.parent, exist_ok=True)
            shutil.move(source, destination)

        # replace usages
        all_sql_files = [
            p
            for project in project_dirs()
            for p in map(Path, glob(f"{project}/**/*.sql", recursive=True))
        ]

        for sql_file in all_sql_files:
            sql = sql_file.read_text()

            replaced_sql = sql.replace(
                f"{old_full_routine_name}(", f"{new_full_routine_name}("
            )
            sql_file.write_text(replaced_sql)

        click.echo(f"Renamed {old_full_routine_name} to {new_full_routine_name}")


mozfun.add_command(copy.copy(rename))
mozfun.commands[
    "rename"
].help = """Rename mozfun routine or mozfun routine dataset.
Replaces all usages in queries with the new name.

Examples:

\b
# Rename routine
./bqetl mozfun rename hist.extract hist.ext

\b
# Rename routine matching a specific pattern
./bqetl mozfun rename *.array_* *.list_*

\b
# Rename routine dataset
./bqetl mozfun rename hist.* histogram.*
"""
