"""bigquery-etl CLI UDF command."""

import click
from fnmatch import fnmatchcase
import os
from pathlib import Path
import pytest
import re
import shutil
import string
import sys
import yaml

from ..cli.utils import is_valid_dir, is_authenticated
from ..format_sql.formatter import reformat
from ..cli.format import format
from ..udf import publish_udfs
from ..docs import validate_docs

UDF_NAME_RE = re.compile(r"^(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)$")
UDF_DATASET_RE = re.compile(r"^(?P<dataset>[a-zA-z0-9_]+)$")
UDF_FILE_RE = re.compile(r"(^.*/|^)([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/udf\.sql$")
DEFAULT_DEPENDENCY_DIR = "udf_js/lib/"
DEFAULT_GCS_BUCKET = "moz-fx-data-prod-bigquery-etl"
DEFAULT_GCS_PATH = ""
DEFAULT_PROJECT_ID = "moz-fx-data-shared-prod"


def _udfs_matching_name_pattern(pattern, udf_paths):
    """Return paths to UDFs matching the name pattern."""
    udf_files = []
    for udf_path in udf_paths:
        all_udf_files = Path(udf_path).rglob("*.sql")

        for udf_file in all_udf_files:
            match = UDF_FILE_RE.match(str(udf_file))
            if match:
                dataset = match.group(2)
                udf_name = match.group(3)
                udf_name = f"{dataset}.{udf_name}"
                if fnmatchcase(udf_name, pattern):
                    udf_files.append(udf_file)

    return udf_files


path_option = click.option(
    "--path",
    "-p",
    help="Path to directory with UDFs. Use default directories if not set.",
    type=click.Path(file_okay=False),
)


@click.group(help="Commands for managing UDFs.")
@click.pass_context
def udf(ctx):
    """Create the CLI group for the UDF command."""
    ctx.ensure_object(dict)
    ctx.obj["UDF_DIRS"] = ("udf", "udf_js")


@click.group(help="Commands for managing mozfun UDFs.")
@click.pass_context
def mozfun(ctx):
    """Create the CLI group for the mozfun command."""
    ctx.ensure_object(dict)
    ctx.obj["UDF_DIRS"] = ("mozfun",)


@udf.command(help="Create a new UDF")
@click.argument("name")
@path_option
@click.pass_context
def create(ctx, name, path):
    """CLI command for creating a new UDF."""
    udf_dir = ctx.obj["UDF_DIRS"][0]
    if path and is_valid_dir(ctx, name, path):
        udf_dir = path

    # create directory structure
    try:
        match = UDF_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")
    except AttributeError:
        click.echo("New UDFs must be named like: <dataset>.<udf_name>")
        sys.exit(1)

    udf_dir = Path(udf_dir)
    if udf_dir.name == dataset:
        udf_path = udf_dir / name
    else:
        udf_path = udf_dir / dataset / name
    udf_path.mkdir(parents=True)

    # create SQL file with UDF definition
    udf_file = udf_path / "udf.sql"
    udf_file.write_text(
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
            SELECT assert_true({dataset}.{name}())
            """
        )
        + "\n"
    )

    # create defaul metadata.yaml
    metadata_file = udf_path / "metadata.yaml"
    metadata = {
        "friendly_name": string.capwords(dataset + " " + name.replace("_", " ")),
        "description": "Please provide a description for the UDF",
    }
    metadata_file.write_text(yaml.dump(metadata))


mozfun.add_command(create)


@udf.command(help="Get UDF information")
@click.argument("name", required=False)
@path_option
@click.option("--usages", "-u", is_flag=True, help="Show UDF usages", default=False)
@click.option(
    "--sql_dir",
    "--sql-dir",
    type=click.Path(file_okay=False),
    callback=is_valid_dir,
    help="Path to SQL files",
    default=".",
)
@click.pass_context
def info(ctx, name, path, usages, sql_dir):
    """CLI command for returning information about UDFs."""
    udf_dirs = ctx.obj["UDF_DIRS"]
    if path and is_valid_dir(None, None, path):
        udf_dirs = (path,)

    if name is None:
        name = "*.*"

    udf_files = _udfs_matching_name_pattern(name, udf_dirs)

    for udf_file in udf_files:
        udf_file_path = Path(udf_file)
        udf_name = udf_file_path.parent.name
        udf_dataset = udf_file_path.parent.parent.name

        try:
            metadata = yaml.safe_load(open(udf_file_path.parent / "metadata.yaml"))
        except FileNotFoundError:
            metadata = None

        click.secho(f"{udf_dataset}.{udf_name}", bold=True)
        click.echo(f"path: {udf_file_path}")

        if metadata is None:
            click.echo("No metadata")
        else:
            click.echo(f"description: {metadata['description']}")

        no_usages = True
        if usages:
            # find UDF usages in SQL files
            click.echo("usages: ")
            sql_files = Path(sql_dir).rglob("*.sql")
            for sql_file in sql_files:
                if f"{udf_dataset}.{udf_name}" in sql_file.read_text():
                    no_usages = False
                    click.echo(f"  {sql_file}")

            if no_usages:
                click.echo("  No usages.")

        click.echo("")


mozfun.add_command(info)


@udf.command(
    help="Validate UDFs.",
)
@click.argument("name", required=False)
@path_option
@click.pass_context
def validate(ctx, name, path):
    """Validate UDFs by formatting and running tests."""
    udf_dirs = ctx.obj["UDF_DIRS"]
    if path and is_valid_dir(None, None, path):
        udf_dirs = (path,)

    if name is None:
        name = "*.*"

    udf_files = _udfs_matching_name_pattern(name, udf_dirs)

    validate_docs.validate(udf_dirs)
    for udf_file in udf_files:
        ctx.invoke(format, path=str(udf_file.parent))
        pytest.main([str(udf_file.parent)])


mozfun.add_command(validate)


@udf.command(
    help="Publish UDFs to BigQuery.",
)
@click.argument("path", type=click.Path(file_okay=False), required=False)
@click.option(
    "--project",
    "-p",
    help="GCP project to publish UDFs to. If not set, uses default project.",
)
@click.option(
    "--dependency-dir",
    "--dependency_dir",
    default=DEFAULT_DEPENDENCY_DIR,
    help="The directory JavaScript dependency files for UDFs are stored.",
)
@click.option(
    "--gcs-bucket",
    "--gcs_bucket",
    default=DEFAULT_GCS_BUCKET,
    help="The GCS bucket where dependency files are uploaded to.",
)
@click.option(
    "--gcs-path",
    "--gcs_path",
    default=DEFAULT_GCS_PATH,
    help="The GCS path in the bucket where dependency files are uploaded to.",
)
@click.pass_context
def publish(ctx, path, project, dependency_dir, gcs_bucket, gcs_path):
    """Publish UDFs."""
    public = False
    if project:
        project_id = project
    else:
        if ctx.obj["UDF_DIRS"] == ("mozfun",):
            project_id = "mozfun"
            public = True
        else:
            project_id = DEFAULT_PROJECT_ID

    if not is_authenticated(project_id):
        click.echo("User needs to be authenticated to publish UDFs.", err=True)
        sys.exit(1)

    udf_dirs = ctx.obj["UDF_DIRS"]
    if path and is_valid_dir(path):
        udf_dirs = path

    click.echo(f"Publish UDFs to {project_id}")
    publish_udfs.publish(
        udf_dirs, project_id, dependency_dir, gcs_bucket, gcs_path, public
    )
    click.echo(f"Published UDFs to {project_id}")


mozfun.add_command(publish)


@udf.command(
    help="Rename UDF or UDF dataset.",
)
@click.argument("name", required=True)
@click.argument("new_name", required=True)
@path_option
@click.option(
    "--sql_path",
    "--sql-path",
    help="Path to directory with SQL queries.",
    type=click.Path(file_okay=False),
    default="sql/",
)
@click.pass_context
def rename(ctx, name, path, new_name, sql_path):
    """Rename UDFs based on pattern."""
    udf_dirs = ctx.obj["UDF_DIRS"]
    if path and is_valid_dir(None, None, path):
        udf_dirs = (path,)

    udf_files = _udfs_matching_name_pattern(name, udf_dirs)

    if UDF_NAME_RE.match(new_name) and len(udf_files) <= 1:
        # rename UDF
        match = UDF_NAME_RE.match(new_name)
        new_udf_name = match.group("name")
        new_udf_dataset = match.group("dataset")
    elif UDF_DATASET_RE.match(new_name):
        # rename UDF dataset
        match = UDF_DATASET_RE.match(new_name)
        new_udf_name = None
        new_udf_dataset = match.group("dataset")
    else:
        click.echo("Invalid rename naming patterns.")
        sys.exit(1)

    for udf_file in udf_files:
        # move to new directories
        old_full_udf_name = f"{udf_file.parent.parent.name}.{udf_file.parent.name}"

        if new_udf_name and new_udf_dataset:
            source = udf_file.parent
            destination = udf_file.parent.parent.parent / new_udf_dataset / new_udf_name
            new_full_udf_name = f"{new_udf_dataset}.{new_udf_name}"
        else:
            source = udf_file.parent.parent
            destination = udf_file.parent.parent.parent / new_udf_dataset
            new_full_udf_name = f"{new_udf_dataset}.{udf_file.parent.name}"

        if source.exists():
            os.makedirs(destination.parent, exist_ok=True)
            shutil.move(source, destination)

        # replace usages
        all_sql_files = list(Path(sql_path).rglob("*.sql"))

        for udf_path in udf_dirs:
            all_sql_files += list(Path(udf_path).rglob("*.sql"))

        for sql_file in all_sql_files:
            sql = sql_file.read_text()

            replaced_sql = sql.replace(f"{old_full_udf_name}(", f"{new_full_udf_name}(")
            sql_file.write_text(replaced_sql)

        click.echo(f"Renamed {old_full_udf_name} to {new_full_udf_name}")


mozfun.add_command(rename)
