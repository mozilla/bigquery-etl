"""Generate documentation for mozfun."""
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


def generate_mozfun_docs(out_dir, project_dir):
    """Generate documentation for mozfun."""
    for root, _dirs, files in os.walk(project_dir):
        if DOCS_FILE in files:
            # copy doc file to output and replace example references
            src = os.path.join(root, DOCS_FILE)
            # remove empty strings from path parts
            path_parts = list(filter(None, root.split(os.sep)))
            name = path_parts[-1]
            path = Path(os.sep.join(path_parts[1:-1]))
            if os.path.split(root)[1] == "":
                # project level-doc file
                project_doc_dir = out_dir / path / name
                project_doc_dir.mkdir(parents=True, exist_ok=True)
                dest = project_doc_dir / "about.md"
                dest.write_text(load_with_examples(src))
            else:
                description = None
                if METADATA_FILE in files:
                    source_link = f"{ConfigLoader.get('docs', 'source_url')}/{root}"
                    edit_link = (
                        f"{ConfigLoader.get('docs', 'edit_url')}/{root}/{METADATA_FILE}"
                    )

                    with open(os.path.join(root, METADATA_FILE)) as stream:
                        try:
                            description = yaml.safe_load(stream).get(
                                "description", None
                            )
                        except yaml.YAMLError:
                            pass
                # dataset or UDF level doc file
                if UDF_FILE in files or PROCEDURE_FILE in files:
                    # UDF-level doc; append to dataset doc
                    dataset_name = os.path.basename(path)
                    dataset_doc = out_dir / path.parent / f"{dataset_name}.md"
                    docfile_content = load_with_examples(src)
                    with open(dataset_doc, "a") as dataset_doc_file:
                        dataset_doc_file.write("\n\n")
                        # Inject a level-2 header with the UDF name & type
                        is_udf = UDF_FILE in files
                        routine_type = "UDF" if is_udf else "Stored Procedure"
                        dataset_doc_file.write(f"## {name} ({routine_type})\n\n")
                        # Inject the "description" from metadata.yaml
                        if description:
                            formated = format_url(description)
                            dataset_doc_file.write(f"{formated}\n\n")
                        # Inject the contents of the README.md
                        dataset_doc_file.write(docfile_content)
                        # Inject input and output parameters from sql
                        if is_udf:
                            with open(os.path.join(root, UDF_FILE), "r") as udf_file:
                                input_str, output_str = get_mozfun_parameters(
                                    udf_file.read()
                                )
                        else:
                            with open(
                                os.path.join(root, PROCEDURE_FILE), "r"
                            ) as procedure_file:
                                input_str, output_str = get_mozfun_parameters(
                                    procedure_file.read()
                                )

                        if input_str or output_str:
                            dataset_doc_file.write("\n### Parameters\n\n")
                            if input_str:
                                dataset_doc_file.write("\n**INPUTS**\n\n")
                                dataset_doc_file.write(f"```\n{input_str}\n```\n\n")
                            if output_str:
                                dataset_doc_file.write("\n**OUTPUTS**\n\n")
                                dataset_doc_file.write(f"```\n{output_str}\n```\n\n")

                        # Add links to source and edit
                        sourced = add_source_and_edit(source_link, edit_link)
                        dataset_doc_file.write(f"{sourced}\n\n")
                else:
                    # dataset-level doc; create a new doc file
                    dest = out_dir / path / f"{name}.md"
                    dest.write_text(load_with_examples(src))
