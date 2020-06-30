"""Generates documentation for a project."""

from argparse import ArgumentParser
import os
from pathlib import Path
import re
import shutil
import yaml

from bigquery_etl.util import standard_args

DEFAULT_PROJECTS = ["mozfun/"]
DOCS_FILE = "README.md"
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


def load_with_examples(file):
    """Load doc file and replace SQL references with examples."""
    with open(file) as doc_file:
        file_content = doc_file.read()

        path, _ = os.path.split(file)

        for sql_ref in re.findall(SQL_REF_RE, file_content):
            sql_example_file = path / Path(sql_ref)
            with open(sql_example_file) as example_sql:
                md_sql = f"```sql\n{example_sql.read()}\n```"
                file_content = file_content.replace(f"@sql({sql_ref})", md_sql)

    return file_content


def generate_docs(out_dir, project_dirs, mkdocs_file):
    """Generate documentation for project."""

    with open(mkdocs_file, "r") as yaml_stream:
        mkdocs = yaml.safe_load(yaml_stream)

        dir_structure = {}

        for project_dir in project_dirs:
            if os.path.isdir(project_dir):
                for root, dirs, files in os.walk(project_dir):
                    if DOCS_FILE in files:
                        Path(os.path.join(out_dir, root)).mkdir(
                            parents=True, exist_ok=True
                        )

                        # copy doc file to output and replace example references
                        src = os.path.join(root, DOCS_FILE)
                        dest = Path(os.path.join(out_dir, root)) / INDEX_MD
                        dest.write_text(load_with_examples(src))

                        # parse the doc directory structure
                        # used in mkdocs.yml
                        path_parts = root.split(os.sep)
                        config = dir_structure

                        for part in path_parts:
                            config = config.setdefault(part, {})

    # convert directory structure to mkdocs compatible format
    if "nav" not in mkdocs:
        mkdocs["nav"] = []

    for project, datasets in dir_structure.items():
        if len(datasets) == 0:
            mkdocs["nav"].append({project: f"{project}/{INDEX_MD}"})
        else:
            dataset_entries = []
            if os.path.isfile(os.path.join(out_dir, project, INDEX_MD)):
                dataset_entries.append({"Overview": f"{project}/{INDEX_MD}"})

            for dataset, artifacts in datasets.items():
                if dataset != "":
                    if len(artifacts) == 0:
                        dataset_entries.append(
                            {dataset: f"{project}/{dataset}/{INDEX_MD}"}
                        )
                    else:
                        artifact_entries = []
                        if os.path.isfile(
                            os.path.join(out_dir, project, dataset, INDEX_MD)
                        ):
                            artifact_entries.append(
                                {"Overview": f"{project}/{dataset}/{INDEX_MD}"}
                            )

                        for artifact, _ in artifacts.items():
                            artifact.append(
                                {artifact: f"{project}/{dataset}/{artifact}/{INDEX_MD}"}
                            )

                        dataset_entries += artifact_entries

            mkdocs["nav"].append({project: dataset_entries})

    # write to mkdocs.yml
    with open(mkdocs_file, "w") as yaml_stream:
        yaml.dump(mkdocs, yaml_stream)


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
    generate_docs(out_dir, args.project_dirs, mkdocs_path)


if __name__ == "__main__":
    main()
