"""Docs."""

import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import rich_click as click

from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import DryRun

EXAMPLE_DIR = "examples"
INDEX_MD = "index.md"


@click.group("docs", help="Commands for generated documentation.")
def docs_():
    """Create the CLI group for doc commands."""
    pass


project_dirs_option = click.option(
    "--project_dirs",
    "--project-dirs",
    help="Directories of projects documentation is generated for.",
    multiple=True,
    default=[
        ConfigLoader.get("default", "sql_dir") + "/" + project + "/"
        for project in ConfigLoader.get("docs", "default_projects")
    ],
)

log_level_option = click.option(
    "--log-level",
    "--log_level",
    help="Log level.",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)


@docs_.command("generate", help="Generate the project docs.")
@project_dirs_option
@click.option(
    "--docs_dir",
    "--docs-dir",
    default=ConfigLoader.get("docs", "docs_dir"),
    help="Directory containing static documentation.",
)
@click.option(
    "--output_dir",
    "--output-dir",
    required=True,
    help="Generated documentation is written to this output directory.",
)
@log_level_option
def generate(project_dirs, docs_dir, output_dir, log_level):
    """Generate documentation for project."""
    from bigquery_etl.docs.bqetl.generate_bqetl_docs import generate_bqetl_docs
    from bigquery_etl.docs.mozfun.generate_mozfun_docs import generate_udf_docs

    out_dir = os.path.join(output_dir, "docs")

    # To customize Mkdocs, we need to extend the theme with an `overrides` folder
    # https://squidfunk.github.io/mkdocs-material/customization/#overriding-partials
    override_dir = os.path.join(output_dir, "overrides")

    if os.path.exists(out_dir) and os.path.exists(override_dir):
        shutil.rmtree(out_dir)
        shutil.rmtree(override_dir)

    # copy assets from /docs and /overrides folders to output folder
    shutil.copytree(docs_dir, out_dir)
    shutil.copytree("bigquery_etl/docs/overrides", override_dir)

    # move mkdocs.yml out of docs/
    mkdocs_path = os.path.join(output_dir, "mkdocs.yml")
    shutil.move(os.path.join(out_dir, "mkdocs.yml"), mkdocs_path)

    # generate bqetl command docs
    generate_bqetl_docs(Path(out_dir) / "bqetl.md")

    # generate docs
    for project_dir in project_dirs:
        generate_udf_docs(out_dir, project_dir)


@docs_.command("validate", help="Validate the project docs.")
@project_dirs_option
@log_level_option
def validate(project_dirs, log_level):
    """Validate UDF docs."""
    from bigquery_etl.routine.parse_routine import read_routine_dir, sub_local_routines

    is_valid = True

    for project_dir in project_dirs:
        if os.path.isdir(project_dir):
            parsed_routines = read_routine_dir(project_dir)

            for root, dirs, files in os.walk(project_dir, followlinks=True):
                if os.path.basename(root) == EXAMPLE_DIR:
                    sql_files = (f for f in files if os.path.splitext(f)[1] == ".sql")
                    for file in sql_files:
                        dry_run_sql = sub_local_routines(
                            (Path(root) / file).read_text(),
                            project_dir,
                            parsed_routines,
                        )

                        # store sql in temporary file for dry_run
                        tmp_dir = Path(tempfile.mkdtemp()) / Path(root)
                        tmp_dir.mkdir(parents=True, exist_ok=True)
                        tmp_example_file = tmp_dir / file
                        tmp_example_file.write_text(dry_run_sql)

                        if not DryRun(str(tmp_example_file)).is_valid():
                            is_valid = False

    if not is_valid:
        print("Invalid examples.")
        sys.exit(1)
