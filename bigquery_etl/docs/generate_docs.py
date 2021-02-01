"""Generates documentations for provided projects."""

from argparse import ArgumentParser
import os
from pathlib import Path
import re
import shutil
from typing import Dict, Sequence

from bigquery_etl.util import standard_args
import yaml


DEFAULT_PROJECTS_DIRS = [
    "sql/mozfun/",
    "sql/moz-fx-data-marketing-prod/",
    "sql/moz-fx-data-shared-prod/",
]
DOCS_FILE = "README.md"
UDF_FILE = "udf.sql"
VIEW_FILE = "view.sql"
PROCEDURE_FILE = "stored_procedure.sql"
METADATA_FILE = "metadata.yaml"
DOCS_DIR = "docs/"
INDEX_MD = "index.md"
SQL_REF_RE = r"@sql\((.+)\)"
SOURCE_URL = "https://github.com/mozilla/bigquery-etl/blob/master"
EDIT_URL = "https://github.com/mozilla/bigquery-etl/edit/master/"

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


def get_markdown_link(title, link):
    """Return markdown link."""
    return f"[{title}]({link})"


def parse_metadata(metadata: Dict) -> Sequence[str]:
    """Parse metadata object to markdown text."""
    output_lines = []
    for key, value in metadata.items():
        if key == "owners":
            emails = ",".join(
                [
                    get_markdown_link(value[i], f"mailto:{value[i]}")
                    for (i, item) in enumerate(value)
                ]
            )
            output_lines.append(
                f'**{key.replace("_", " ").capitalize()}**: {emails}\n\n'
            )
        elif isinstance(value, dict):
            if "dag_name" in value:
                dag_link = get_markdown_link(
                    value["dag_name"], f"{SOURCE_URL}/dags/{value['dag_name']}.py"
                )
                output_lines.append(f"**DAG name**: {dag_link}\n\n")
            if "schedule" in value:
                output_lines.append(f'**Schedule**: {value["schedule"]}\n\n')
        else:
            output_lines.append(
                f'**{key.replace("_", " ").capitalize()}**: {value}\n\n'
            )

    return output_lines


def parse_sql(sql_file):
    """Parse SQL content to markdown code block."""
    return f"~~~~sql\n{sql_file}~~~~\n"


def main():
    """Generate documentation for project."""
    args = parser.parse_args()
    out_dir = os.path.join(args.output_dir, "docs")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    shutil.copytree(args.docs_dir, out_dir)

    # move mkdocs.yml out of docs/
    mkdocs_path = os.path.join(args.output_dir, "mkdocs.yml")
    shutil.move(os.path.join(out_dir, "mkdocs.yml"), mkdocs_path)

    # move files to docs/
    for project_dir in args.project_dirs:
        if os.path.isdir(project_dir):
            # generate docs for mozfun UDFs
            if project_dir == "sql/mozfun/":
                for root, dirs, files in os.walk(project_dir):
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
                                dataset_doc = (
                                    out_dir / path.parent / f"{dataset_name}.md"
                                )
                                docfile_content = load_with_examples(src)
                                with open(dataset_doc, "a") as dataset_doc_file:
                                    dataset_doc_file.write("\n\n")
                                    # Inject a level-2 header with the UDF name & type
                                    is_udf = UDF_FILE in files
                                    routine_type = (
                                        "UDF" if is_udf else "Stored Procedure"
                                    )
                                    dataset_doc_file.write(
                                        f"## {name} ({routine_type})\n\n"
                                    )
                                    # Inject the "description" from metadata.yaml
                                    if description:
                                        formated = format_url(description)
                                        dataset_doc_file.write(f"{formated}\n\n")
                                    # Inject the contents of the README.md
                                    dataset_doc_file.write(docfile_content)
                                    # Add links to source and edit
                                    sourced = add_source_and_edit(
                                        source_link, edit_link
                                    )
                                    dataset_doc_file.write(f"{sourced}\n\n")
                            else:
                                # dataset-level doc; create a new doc file
                                dest = out_dir / path / f"{name}.md"
                                dest.write_text(load_with_examples(src))
            # generate docs for Mozdata
            else:
                project_doc_dir = Path(os.path.join(out_dir, project_dir.split("/")[1]))
                project_doc_dir.mkdir(parents=True, exist_ok=True)

                # generate a list of datasets (ignore *_derived dirs)
                data_sets = [
                    item
                    for item in os.listdir(project_dir)
                    if os.path.isdir(os.path.join(project_dir, item))
                    and "derived" not in item
                ]

                for table in data_sets:
                    output = []
                    with open(project_doc_dir / f"{table}.md", "w") as dataset_doc:
                        # Manually set the page title to prevent mkdocs
                        # from removing underscores in data sets' name
                        # https://www.mkdocs.org/user-guide/writing-your-docs/#meta-data
                        output.append(f"---\ntitle: {table}\n---\n\n")
                        for root, dirs, files in os.walk(f"{project_dir}{table}"):
                            if dirs == []:
                                dataset_name = root.split("/")[-1]
                                source_link = f"{SOURCE_URL}/{root}"
                                # link to data set directory on GitHub
                                output.append(
                                    f"##{get_markdown_link(dataset_name, source_link)}\n"
                                )

                            if METADATA_FILE in files:
                                # link to metadata.yaml on Github
                                file_url = f"{SOURCE_URL}/{root}/{METADATA_FILE}"
                                output.append(
                                    f'### {get_markdown_link("Metadata", file_url)}\n\n'
                                )
                                with open(os.path.join(root, METADATA_FILE)) as stream:
                                    try:
                                        for line in parse_metadata(
                                            yaml.safe_load(stream)
                                        ):
                                            output.append(line)
                                    except yaml.YAMLError:
                                        pass
                            if VIEW_FILE in files:
                                # link to view.sql on Github
                                file_url = f"{SOURCE_URL}/{root}/{VIEW_FILE}"
                                output.append(
                                    f'### {get_markdown_link("View", file_url)}\n\n'
                                )
                                with open(os.path.join(root, VIEW_FILE)) as stream:
                                    try:
                                        for line in parse_sql(stream.read()):
                                            output.append(line)
                                    except yaml.YAMLError:
                                        pass
                        dataset_doc.writelines(output)


if __name__ == "__main__":
    main()
