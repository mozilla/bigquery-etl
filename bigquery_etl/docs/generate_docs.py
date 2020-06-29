"""Generates documentation for a project."""

from argparse import ArgumentParser
from jinja2 import Environment, PackageLoader
import os
from pathlib import Path
import re

from bigquery_etl.util import standard_args

DEFAULT_PROJECTS = ["mozfun/"]
DOCS_FILE = "README.md"
MKDOCS_CONFIG_TEMPLATE = "mkdocs.j2"
SQL_REF_RE = "@sql\((.+)\)"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_dirs",
    "--project-dirs",
    help="Directories of projects documentation is generated for.",
    nargs="+",
    default=DEFAULT_PROJECTS,
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

        path_parts = file.split(os.sep)

        path, _ = os.path.split(file)

        for sql_ref in re.findall(SQL_REF_RE, file_content):
            sql_example_file = path / Path(sql_ref)
            with open(sql_example_file) as example_sql:
                md_sql = f"```sql\n{example_sql.read()}\n```"
                file_content = file_content.replace(f"@sql({sql_ref})", md_sql)

    return file_content


def main():
    """Generate documentation for project."""
    args = parser.parse_args()
    out_dir = args.output_dir

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    dir_structure = {}

    for project_dir in args.project_dirs:
        if os.path.isdir(project_dir):
            for root, dirs, files in os.walk(project_dir):
                if DOCS_FILE in files:
                    Path(os.path.join(out_dir, root)).mkdir(parents=True, exist_ok=True)

                    # copy doc file to output and replace example references
                    src = os.path.join(root, DOCS_FILE)
                    dest = Path(os.path.join(out_dir, root)) / "index.md"
                    dest.write_text(load_with_examples(src))

                    # parse the doc directory structure
                    # used in Jinja template to generate nav
                    path_parts = root.split(os.sep)
                    config = dir_structure

                    for part in path_parts:
                        config = config.setdefault(part, {})

    # generate mkdocs.yml
    env = Environment(loader=PackageLoader("bigquery_etl", "docs/templates"))
    mkdocs_template = env.get_template(MKDOCS_CONFIG_TEMPLATE)
    mkdocs_file = Path(args.output_dir) / "mkdocs.yml"

    mkdocs_file.write_text(mkdocs_template.render({"dir_structure": dir_structure}))


if __name__ == "__main__":
    main()
