"""Generates documentations for provided projects."""

import os
import shutil
from argparse import ArgumentParser
from pathlib import Path

from bigquery_etl.docs.bqetl.generate_bqetl_docs import generate_bqetl_docs
from bigquery_etl.docs.derived_datasets.generate_derived_dataset_docs import (
    generate_derived_dataset_docs,
)
from bigquery_etl.docs.mozfun.generate_mozfun_docs import generate_mozfun_docs
from bigquery_etl.util import standard_args

DEFAULT_PROJECTS_DIRS = ["sql/mozfun/", "sql/moz-fx-data-shared-prod/"]
DOCS_DIR = "docs/"
INDEX_MD = "index.md"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_dirs",
    "--project-dirs",
    help="Directories of projects documentation is generated for.",
    nargs="+",
    default=DEFAULT_PROJECTS_DIRS,
)
parser.add_argument(
    "--docs_dir",
    "--docs-dir",
    default=DOCS_DIR,
    help="Directory containing static documentation.",
)
parser.add_argument(
    "--output_dir",
    "--output-dir",
    required=True,
    help="Generated documentation is written to this output directory.",
)
standard_args.add_log_level(parser)


def main():
    """Generate documentation for project."""
    args = parser.parse_args()
    out_dir = os.path.join(args.output_dir, "docs")

    # To customize Mkdocs, we need to extend the theme with an `overrides` folder
    # https://squidfunk.github.io/mkdocs-material/customization/#overriding-partials
    override_dir = os.path.join(args.output_dir, "overrides")

    if os.path.exists(out_dir) and os.path.exists(override_dir):
        shutil.rmtree(out_dir)
        shutil.rmtree(override_dir)

    # copy assets from /docs and /overrides folders to output folder
    shutil.copytree(args.docs_dir, out_dir)
    shutil.copytree("bigquery_etl/docs/overrides", override_dir)

    # move mkdocs.yml out of docs/
    mkdocs_path = os.path.join(args.output_dir, "mkdocs.yml")
    shutil.move(os.path.join(out_dir, "mkdocs.yml"), mkdocs_path)

    # generate bqetl command docs
    generate_bqetl_docs(Path(out_dir) / "bqetl.md")

    # move files to docs/
    for project_dir in args.project_dirs:
        if not os.path.isdir(project_dir):
            continue

        if "mozfun" in project_dir:
            generate_mozfun_docs(out_dir, project_dir)
        else:
            generate_derived_dataset_docs(out_dir, project_dir)


if __name__ == "__main__":
    main()
