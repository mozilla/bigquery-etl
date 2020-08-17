"""Generates documentations for provided projects."""

from argparse import ArgumentParser
import os
from pathlib import Path
import re
import shutil

from bigquery_etl.util import standard_args
import yaml

DEFAULT_PROJECTS = ["mozfun/"]
DOCS_FILE = "README.md"
UDF_FILE = "udf.sql"
METADATA_FILE = "metadata.yaml"
DOCS_DIR = "docs/"
INDEX_MD = "index.md"
SQL_REF_RE = r"@sql\((.+)\)"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_dirs",
    "--project-dirs",
    help="Directories of projects documentation is generated for.",
    nargs="+",
    default=DEFAULT_PROJECTS,
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
            for root, dirs, files in os.walk(project_dir):
                if DOCS_FILE in files:
                    # copy doc file to output and replace example references
                    src = os.path.join(root, DOCS_FILE)
                    # remove empty strings from path parts
                    path_parts = list(filter(None, root.split(os.sep)))
                    name = path_parts[-1]
                    path = Path(os.sep.join(path_parts[:-1]))

                    if os.path.split(root)[1] == "":
                        # project level-doc file
                        project_doc_dir = out_dir / path / name
                        project_doc_dir.mkdir(parents=True, exist_ok=True)
                        dest = project_doc_dir / "overview.md"
                        dest.write_text(load_with_examples(src))
                    else:
                        description = None
                        if METADATA_FILE in files:
                            with open(os.path.join(root, METADATA_FILE)) as stream:
                                try:
                                    description = yaml.safe_load(stream).get(
                                        "description", None
                                    )
                                except yaml.YAMLError:
                                    pass
                        # dataset or UDF level doc file
                        if UDF_FILE in files:
                            # UDF-level doc; append to dataset doc
                            dataset_name = os.path.basename(path)
                            dataset_doc = out_dir / path.parent / f"{dataset_name}.md"
                            docfile_content = load_with_examples(src)
                            with open(dataset_doc, "a") as dataset_doc_file:
                                dataset_doc_file.write("\n\n")
                                # Inject a level-2 header with the UDF name
                                dataset_doc_file.write(f"## {name}\n\n")
                                # Inject the "description" from metadata.yaml
                                if description:
                                    formated = format_url(description)
                                    dataset_doc_file.write(f"{formated}\n\n")
                                # Inject the contents of the README.md
                                dataset_doc_file.write(docfile_content)
                        else:
                            # dataset-level doc; create a new doc file
                            dest = out_dir / path / f"{name}.md"
                            dest.write_text(load_with_examples(src))


if __name__ == "__main__":
    main()
