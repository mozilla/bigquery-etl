"""Generate documentation for mozfun."""
import os
import re
from pathlib import Path

import yaml

DOCS_FILE = "README.md"
METADATA_FILE = "metadata.yaml"
SOURCE_URL = "https://github.com/mozilla/bigquery-etl/blob/generated-sql"
EDIT_URL = "https://github.com/mozilla/bigquery-etl/edit/generated-sql"
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
                    source_link = f"{SOURCE_URL}/{root}"
                    edit_link = f"{EDIT_URL}/{root}/{METADATA_FILE}"

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
                        if is_udf:
                            found_input = False
                            input_lines = []
                            found_return = False
                            return_lines = []
                            with open(os.path.join(root, UDF_FILE), "r") as udf_sql:
                                udf_content = [
                                    line
                                    for line in udf_sql.readlines()
                                    if not line.startswith("#")
                                ]

                                FUNCTION_HEADER = (
                                    f"CREATE OR REPLACE FUNCTION {dataset_name}.{name}("
                                )
                                for line in udf_content:
                                    # FUNCTION_HEADER starts the UDF and then contains inputs in ()
                                    if line.startswith(FUNCTION_HEADER) and len(
                                        line.strip()
                                    ) > len(FUNCTION_HEADER):
                                        input_lines.append(line)
                                    elif line.startswith(FUNCTION_HEADER):
                                        found_input = True
                                        input_lines.append(line)

                                    if (
                                        found_input
                                        and ") AS (" not in line
                                        and not line.startswith(FUNCTION_HEADER)
                                        and "RETURNS" not in line
                                    ):
                                        input_lines.append(line)
                                    elif found_input and (
                                        ") AS (" in line or "RETURNS" in line
                                    ):
                                        found_input = False

                                    # TODO: find_block function? def find_block(start_word, end_word)

                                    # RETURNS has the output types
                                    if line.startswith("RETURNS") and "AS" not in line:
                                        found_return = True
                                        return_lines.append(line)
                                    elif (
                                        "AS" in line
                                        and not line.startswith(FUNCTION_HEADER)
                                        and "RETURNS" in line
                                    ):
                                        return_lines.append(line)

                                    if found_return and "AS" not in line:
                                        return_lines.append(line)
                                    elif found_return and "AS" in line:
                                        found_return = False

                            # assemble, format, write inputs and outputs
                            if len(input_lines) > 0:
                                formatted_inputs = ""
                                for ip in input_lines:
                                    if len(ip) < 5:
                                        continue
                                    if FUNCTION_HEADER in ip and ") AS (" in ip:
                                        formatted_inputs = (
                                            "\n"
                                            + ip.strip()
                                            .removeprefix(FUNCTION_HEADER)
                                            .removesuffix(") AS (")
                                        )
                                    elif FUNCTION_HEADER in ip:
                                        continue
                                    elif "AS" in ip:
                                        break
                                    else:
                                        formatted_inputs = (
                                            formatted_inputs
                                            + "\n"
                                            + ip.strip().rstrip(",")
                                        )

                                formatted_inputs = f"```{formatted_inputs}\n```\n\n"

                                dataset_doc_file.write("INPUTS\n\n")
                                dataset_doc_file.write(formatted_inputs)
                            else:
                                formatted_inputs = None
                                input_lines = []
                                found_input = False

                            if len(return_lines) > 0:
                                # formatted_outputs = "\n".join(return_lines)

                                formatted_outputs = ""
                                for ol in return_lines:
                                    if "RETURNS" in ol and " AS (" in ol:
                                        formatted_outputs = (
                                            ol.strip()
                                            .removeprefix("RETURNS")
                                            .removesuffix(" AS (")
                                            .strip()
                                        )
                                    elif "RETURNS" in ol and len(ol) > len("RETURNS"):
                                        formatted_outputs = (
                                            ol.strip()
                                            .removeprefix("RETURNS")
                                            .removesuffix(" AS (")
                                            .strip()
                                        )
                                    elif "RETURNS" in ol:
                                        continue
                                    elif "AS" in ol:
                                        break
                                    else:
                                        formatted_outputs = (
                                            formatted_outputs
                                            + "\n"
                                            + ol.strip().rstrip(",")
                                        )

                                formatted_outputs = f"```\n{formatted_outputs}\n```\n\n"

                                dataset_doc_file.write("OUTPUTS\n\n")
                                dataset_doc_file.write(formatted_outputs)
                            else:
                                formatted_outputs = None
                                return_lines = []
                                found_return = False

                            # Add links to source and edit
                        sourced = add_source_and_edit(source_link, edit_link)
                        dataset_doc_file.write(f"{sourced}\n\n")
                else:
                    # dataset-level doc; create a new doc file
                    dest = out_dir / path / f"{name}.md"
                    dest.write_text(load_with_examples(src))
