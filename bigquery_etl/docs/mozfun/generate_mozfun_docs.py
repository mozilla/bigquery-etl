"""Generate documentation for mozfun."""
import itertools
import os
import re
from pathlib import Path

import yaml

from bigquery_etl.config import ConfigLoader
from bigquery_etl.util.mozfun_docs_functions import get_mozfun_parameters

DOCS_FILE = "README.md"
METADATA_FILE = "metadata.yaml"
UDF_FILE = "udf.sql"
PROCEDURE_FILE = "stored_procedure.sql"
SQL_REF_RE = r"@sql\((.+)\)"


def format_url(doc):
    """Create links for urls in documentation."""
    doc = re.sub(r"(?<!\()(https?://[^\s]+)(?!\))", r"<\1>", doc)
    return doc


def load_with_examples(file):
    """Load doc file and replace SQL references with examples."""
    with open(file) as doc_file:
        file_content = doc_file.read()

        path, _ = os.path.split(file)

        for sql_ref in re.findall(SQL_REF_RE, file_content):
            sql_example_file = path / Path(sql_ref)
            with open(sql_example_file) as example_sql:
                md_sql = f"```sql\n{example_sql.read().strip()}\n```"
                file_content = file_content.replace(f"@sql({sql_ref})", md_sql)

    return file_content


def add_source_and_edit(source_url, edit_url):
    """Add links to the function directory and metadata.yaml editor."""
    return f"[Source]({source_url})  |  [Edit]({edit_url})"


def _gen_udf_content(udf_file_path: Path) -> str:
    """Generate markdown documentation content for a udf.sql file."""
    udf_path = udf_file_path.parent
    routine_type = "Stored Procedure" if udf_file_path.name == PROCEDURE_FILE else "UDF"
    udf_content = f"## {udf_path.name} ({routine_type})\n\n"

    metadata, source_link, edit_link = None, None, None
    if (metadata_file := udf_path / "metadata.yaml").exists():
        metadata = yaml.safe_load(metadata_file.read_text())

    if metadata is not None:
        if (description := metadata.get("description")) is not None:
            udf_content += f"{format_url(description)}\n\n"
        source_link = f"{ConfigLoader.get('docs', 'source_url')}/{udf_path}"
        edit_link = f"{ConfigLoader.get('docs', 'edit_url')}/{udf_path}/{METADATA_FILE}"

    if (readme := udf_path / DOCS_FILE).exists():
        udf_content += f"{load_with_examples(readme)}\n\n"

    input_str, output_str = get_mozfun_parameters(udf_file_path.read_text())
    if input_str or output_str:
        udf_content += "\n### Parameters\n\n"
        if input_str:
            udf_content += f"\n**INPUTS**\n\n```\n{input_str}\n```\n\n"
        if output_str:
            udf_content += f"\n**OUTPUTS**\n\n```\n{output_str}\n```\n\n"

    if source_link is not None and edit_link is not None:
        udf_content += f"{add_source_and_edit(source_link, edit_link)}\n\n"

    return udf_content


def generate_udf_docs(out_dir: str, project_dir: str) -> None:
    """Generate documentation for UDFs."""
    project_path = Path(project_dir)
    target_path = Path(out_dir) / project_path.name
    target_path.mkdir(parents=True, exist_ok=True)

    if (project_docs := project_path / DOCS_FILE).exists():
        (target_path / "about.md").write_text(project_docs.read_text())

    # Group by dataset to generate one markdown file per BQ dataset with its UDFs:
    for dataset_path, udf_paths in itertools.groupby(
        sorted(
            list(project_path.glob(f"*/*/{UDF_FILE}"))
            + list(project_path.glob(f"*/*/{PROCEDURE_FILE}"))
        ),
        lambda path: path.parent.parent,  # path is project/dataset/udf/udf.sql
    ):
        if not (udfs_content := (_gen_udf_content(path) for path in udf_paths)):
            continue

        file_content = ""
        if (dataset_docs := dataset_path / DOCS_FILE).exists():
            file_content += f"{dataset_docs.read_text()}\n"
        file_content += "\n".join(udfs_content)
        (target_path / f"{dataset_path.name}.md").write_text(file_content)
