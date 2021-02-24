"""Generate documentation for derived datasets."""

import os
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.dryrun import DryRun

VIEW_FILE = "view.sql"
METADATA_FILE = "metadata.yaml"
NON_USER_FACING_DATASET_SUFFIXES = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
)
SOURCE_URL = "https://github.com/mozilla/bigquery-etl/blob/generated-sql"


def generate_derived_dataset_docs(out_dir, project_dir):
    """Generate documentation for derived datasets."""
    project_doc_dir = Path(out_dir) / "mozdata"

    # get a list of all user-facing datasets
    data_sets = [
        item
        for item in os.listdir(project_dir)
        if os.path.isdir(os.path.join(project_dir, item))
        and all(name not in item for name in NON_USER_FACING_DATASET_SUFFIXES)
    ]

    for table in data_sets:
        output = []
        source_urls = {}
        with open(project_doc_dir / f"{table}.md", "w") as dataset_doc:
            # Manually set title to prevent Mkdocs from removing
            # underscores and capitalizing file names
            # https://github.com/mkdocs/mkdocs/issues/1915#issuecomment-561311801
            dataset_doc.write(f"---\ntitle: {table}\n---\n\n")

            for root, dirs, files in os.walk(Path(project_dir) / table):
                # show views in an alphabetical order
                dirs.sort()
                if dirs:
                    continue
                dataset_name = root.split("/")[-1]
                source_urls["Source Directory"] = f"{SOURCE_URL}/{root}"

                metadata = {}
                if METADATA_FILE in files:
                    source_urls[
                        "Metadata File"
                    ] = f"{SOURCE_URL}/{root}/{METADATA_FILE}"
                    with open(os.path.join(root, METADATA_FILE)) as stream:
                        try:
                            metadata = yaml.safe_load(stream)
                        except yaml.YAMLError as error:
                            print(error)
                if VIEW_FILE in files:
                    source_urls["View Definition"] = f"{SOURCE_URL}/{root}/{VIEW_FILE}"
                    view_file = os.path.join(root, VIEW_FILE)

                    referenced_tables = DryRun(
                        sqlfile=view_file, strip_dml=True
                    ).get_referenced_tables()
                file_loader = FileSystemLoader(
                    "bigquery_etl/docs/derived_datasets/templates"
                )
                # Set up a new template environment
                env = Environment(loader=file_loader)
                # Create template with the markdown source text
                template = env.get_template("table.md")

                output = template.render(
                    metadata=metadata,
                    table_name=dataset_name,
                    source_urls=source_urls,
                    referenced_tables=referenced_tables,
                    project_url=f"{SOURCE_URL}/sql",
                )
                dataset_doc.write(output)
