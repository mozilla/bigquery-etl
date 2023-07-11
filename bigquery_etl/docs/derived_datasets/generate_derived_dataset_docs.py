"""Generate documentation for derived datasets."""
import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.dependency import extract_table_references
from bigquery_etl.metadata.parse_metadata import DatasetMetadata, Metadata
from bigquery_etl.schema import Schema

logging.basicConfig(format="%(levelname)s (%(filename)s:%(lineno)d) - %(message)s")

VIEW_FILE = "view.sql"
METADATA_FILE = "metadata.yaml"
SCHEMA_FILE = "schema.yaml"
DATASET_METADATA_FILE = "dataset_metadata.yaml"
README_FILE = "README.md"


def _get_metadata(path, metadata_filename=METADATA_FILE):
    metadata_path = path / metadata_filename
    try:
        if metadata_filename == METADATA_FILE:
            metadata = Metadata.from_file(metadata_path)
            return metadata
        elif metadata_filename == DATASET_METADATA_FILE:
            metadata = DatasetMetadata.from_file(metadata_path)
            return metadata
        else:
            raise Exception(f"Invalid metadata filename provided - {metadata_filename}")
    except FileNotFoundError:
        logging.warning(f"Metadata not found at {str(metadata_path)}")


def _get_readme_content(path):
    readme_file = path / README_FILE
    if readme_file.exists():
        return readme_file.read_text()


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


def _get_schema(table_path):
    schema_path = table_path / SCHEMA_FILE
    try:
        schema = Schema.from_schema_file(schema_path)
        return schema.schema.get("fields")
    except Exception as e:
        logging.warning(f"Unable to open schema: {e}")


def _iter_table_markdown(table_paths, template):
    source_url = ConfigLoader.get("docs", "source_url")
    for table_path in table_paths:
        source_urls = {"Source Directory": f"{source_url}/{str(table_path)}"}

        referenced_tables = _get_referenced_tables_from_view(table_path)
        if referenced_tables:
            source_urls[
                "View Definition"
            ] = f"{source_url}/{str(table_path / VIEW_FILE)}"

        metadata = _get_metadata(table_path)
        if metadata:
            source_urls[
                "Metadata File"
            ] = f"{source_url}/{str(table_path / METADATA_FILE)}"

        readme_content = _get_readme_content(table_path)
        schema = _get_schema(table_path)

        output = template.render(
            metadata=metadata,
            readme_content=readme_content,
            schema=schema,
            table_name=table_path.name,
            qualified_table_name=f"{table_path.parent.name}.{table_path.name}",
            source_urls=source_urls,
            referenced_tables=referenced_tables,
            project_url=f"{source_url}/sql",
        )

        yield output


def generate_derived_dataset_docs(out_dir, project_dir):
    """Generate documentation for derived datasets."""
    output_path = Path(out_dir) / ConfigLoader.get(
        "default", "user_facing_project", fallback="mozdata"
    )
    project_path = Path(project_dir)

    # get a list of all user-facing datasets
    dataset_paths = sorted(
        [
            dataset_path
            for dataset_path in project_path.iterdir()
            if dataset_path.is_dir()
            and all(
                suffix not in str(dataset_path)
                for suffix in ConfigLoader.get(
                    "default", "non_user_facing_dataset_suffixes", fallback=[]
                )
            )
        ]
    )

    for dataset_path in dataset_paths:
        table_paths = sorted([path for path in dataset_path.iterdir() if path.is_dir()])

        file_loader = FileSystemLoader("bigquery_etl/docs/derived_datasets/templates")
        env = Environment(loader=file_loader)
        table_template = env.get_template("table.md")
        dataset_header_template = env.get_template("dataset_header.md")

        dataset_metadata = _get_metadata(
            dataset_path, metadata_filename=DATASET_METADATA_FILE
        )
        dataset_readme_content = _get_readme_content(dataset_path)

        with open(output_path / f"{dataset_path.name}.md", "w") as dataset_doc:
            # In the template, we manually set title to prevent Mkdocs from removing
            # underscores and capitalizing file names
            # https://github.com/mkdocs/mkdocs/issues/1915#issuecomment-561311801
            dataset_header = dataset_header_template.render(
                title=dataset_metadata.friendly_name
                if dataset_metadata
                else dataset_path.name,
                description=dataset_metadata.description if dataset_metadata else None,
                readme_content=dataset_readme_content,
                source_url=f"{ConfigLoader.get('docs', 'source_url')}/{str(dataset_path)}",
            )

            dataset_doc.write(dataset_header)
            dataset_doc.write(
                "".join(_iter_table_markdown(table_paths, table_template))
            )
