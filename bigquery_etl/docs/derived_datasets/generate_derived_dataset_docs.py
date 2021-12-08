"""Generate documentation for derived datasets."""

from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.dependency import extract_table_references

VIEW_FILE = "view.sql"
METADATA_FILE = "metadata.yaml"
NON_USER_FACING_DATASET_SUFFIXES = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
)
SOURCE_URL = "https://github.com/mozilla/bigquery-etl/blob/generated-sql"


def _get_metadata(table_path):
    metadata = {}
    metadata_file = table_path / METADATA_FILE
    if metadata_file.exists():
        with open(metadata_file) as stream:
            try:
                metadata = yaml.safe_load(stream)
            except yaml.YAMLError as error:
                print(error)
    return metadata


def _get_referenced_tables_from_view(table_path):
    referenced_tables = []
    view_file = table_path / VIEW_FILE
    if view_file.exists():
        for referenced_table in extract_table_references(view_file.read_text()):
            table_split = referenced_table.split(".")
            if len(table_split) == 2:
                # missing project ID, retrieve from file path
                [dataset_id, table_id] = table_split
                project_id = view_file.parent.parent.parent.name
            elif len(table_split) == 3:
                [project_id, dataset_id, table_id] = table_split
            else:
                continue

            referenced_tables.append(
                {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                }
            )
    return referenced_tables


def _iter_table_markdown(table_paths, template):
    for table_path in table_paths:
        source_urls = {"Source Directory": f"{SOURCE_URL}/{str(table_path)}"}

        referenced_tables = _get_referenced_tables_from_view(table_path)
        if referenced_tables:
            source_urls[
                "View Definition"
            ] = f"{SOURCE_URL}/{str(table_path / VIEW_FILE)}"

        metadata = _get_metadata(table_path)
        if metadata:
            source_urls[
                "Metadata File"
            ] = f"{SOURCE_URL}/{str(table_path / METADATA_FILE)}"

        output = template.render(
            metadata=metadata,
            table_name=table_path.name,
            source_urls=source_urls,
            referenced_tables=referenced_tables,
            project_url=f"{SOURCE_URL}/sql",
        )

        yield output


def generate_derived_dataset_docs(out_dir, project_dir):
    """Generate documentation for derived datasets."""
    output_path = Path(out_dir) / "datasets"
    project_path = Path(project_dir)

    # get a list of all user-facing datasets
    dataset_paths = sorted(
        [
            dataset_path
            for dataset_path in project_path.iterdir()
            if dataset_path.is_dir()
            and all(
                suffix not in str(dataset_path)
                for suffix in NON_USER_FACING_DATASET_SUFFIXES
            )
        ]
    )

    for dataset_path in dataset_paths:
        table_paths = sorted([path for path in dataset_path.iterdir() if path.is_dir()])

        file_loader = FileSystemLoader("bigquery_etl/docs/derived_datasets/templates")
        env = Environment(loader=file_loader)
        template = env.get_template("table.md")

        with open(output_path / f"{dataset_path.name}.md", "w") as dataset_doc:
            # Manually set title to prevent Mkdocs from removing
            # underscores and capitalizing file names
            # https://github.com/mkdocs/mkdocs/issues/1915#issuecomment-561311801
            dataset_doc.write(f"---\ntitle: {dataset_path.name}\n---\n\n")

            dataset_doc.write("".join(_iter_table_markdown(table_paths, template)))
