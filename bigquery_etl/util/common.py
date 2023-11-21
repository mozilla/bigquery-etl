"""Generic utility functions."""
import glob
import logging
import os
import random
import re
import string
import tempfile
import warnings
from pathlib import Path
from typing import List
from uuid import uuid4

from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metrics import MetricHub

# Search for all camelCase situations in reverse with arbitrary lookaheads.
REV_WORD_BOUND_PAT = re.compile(
    r"""
    \b                                  # standard word boundary
    |(?<=[a-z][A-Z])(?=\d*[A-Z])        # A7Aa -> A7|Aa boundary
    |(?<=[a-z][A-Z])(?=\d*[a-z])        # a7Aa -> a7|Aa boundary
    |(?<=[A-Z])(?=\d*[a-z])             # a7A -> a7|A boundary
    """,
    re.VERBOSE,
)
FILE_PATH = Path(os.path.dirname(__file__))
DEFAULT_QUERY_TEMPLATE_VARS = {
    "is_init": lambda: False,
    "metrics": MetricHub(),
}
ROOT = Path(__file__).parent.parent.parent
CHECKS_MACROS_DIR = ROOT / "tests" / "checks"


def snake_case(line: str) -> str:
    """Convert a string into a snake_cased string."""
    # replace non-alphanumeric characters with spaces in the reversed line
    subbed = re.sub(r"[^\w]|_", " ", line[::-1])
    # apply the regex on the reversed string
    words = REV_WORD_BOUND_PAT.split(subbed)
    # filter spaces between words and snake_case and reverse again
    return "_".join([w.lower() for w in words if w.strip()])[::-1]


def project_dirs(project_id=None, sql_dir=None) -> List[str]:
    """Return all project directories."""
    if sql_dir is None:
        sql_dir = ConfigLoader.get("default", "sql_dir", fallback="sql")
    if project_id is None:
        return [
            os.path.join(sql_dir, project_dir) for project_dir in os.listdir(sql_dir)
        ]
    else:
        return [os.path.join(sql_dir, project_id)]


def random_str(length: int = 12) -> str:
    """Return a random string of the specified length."""
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def render(
    sql_filename,
    template_folder=".",
    format=True,
    imports=[],
    **kwargs,
) -> str:
    """Render a given template query using Jinja."""
    path = Path(template_folder) / sql_filename
    skip = {
        file
        for skip in ConfigLoader.get("render", "skip", fallback=[])
        for file in glob.glob(
            skip,
            recursive=True,
        )
    }
    test_project = ConfigLoader.get("default", "test_project")
    sql_dir = ConfigLoader.get("default", "sql_dir", fallback="sql")

    if test_project in str(path):
        # check if staged file needs to be skipped
        skip.update(
            [
                p
                for f in [Path(s) for s in skip]
                for p in glob.glob(
                    f"{sql_dir}/{test_project}/{f.parent.parent.name}*/{f.parent.name}/{f.name}",
                    recursive=True,
                )
            ]
        )

    if any(s in str(path) for s in skip):
        rendered = path.read_text()
    else:
        if sql_filename == "checks.sql":
            # make macros available by creating a temporary file and pasting macro definition
            # alongside checks.
            # it is not possible to use `include` or `import` since the macros live in a different
            # directory than the checks Jinja template.
            with tempfile.NamedTemporaryFile(mode="w+") as tmp_checks_file:
                macro_imports = "\n".join(
                    [
                        macro_file.read_text()
                        for macro_file in CHECKS_MACROS_DIR.glob("*")
                    ]
                )
                checks_template = Path(str(tmp_checks_file.name))
                checks_template.write_text(
                    macro_imports
                    + "\n"
                    + (Path(template_folder) / sql_filename).read_text()
                )

                file_loader = FileSystemLoader(f"{str(checks_template.parent)}")
                env = Environment(loader=file_loader)
                main_sql = env.get_template(checks_template.name)
        else:
            file_loader = FileSystemLoader(f"{template_folder}")
            env = Environment(loader=file_loader)
            main_sql = env.get_template(sql_filename)

        template_vars = DEFAULT_QUERY_TEMPLATE_VARS | kwargs
        rendered = main_sql.render(**template_vars)

    if format:
        rendered = reformat(rendered)
    return rendered


def get_table_dir(output_dir, full_table_id):
    """Return the output directory for a given table id."""
    return Path(os.path.join(output_dir, *list(full_table_id.split(".")[-2:])))


def write_sql(output_dir, full_table_id, basename, sql, skip_existing=False):
    """Write out a query to a location based on the table ID.

    :param output_dir:    Base target directory (probably sql/moz-fx-data-shared-prod/)
    :param full_table_id: Table ID in project.dataset.table form
    :param basename:      The name to give the written file (like query.sql)
    :param sql:           The query content to write out
    :param skip_existing: Whether to skip an existing file rather than overwriting it
    """
    d = get_table_dir(output_dir, full_table_id)
    d.mkdir(parents=True, exist_ok=True)
    target = d / basename
    if skip_existing and target.exists():
        logging.info(f"Not writing {target} because it already exists")
        return
    logging.info(f"Writing {target}")
    with target.open("w") as f:
        f.write(sql)
        f.write("\n")


class TempDatasetReference(bigquery.DatasetReference):
    """Extend DatasetReference to simplify generating temporary tables."""

    def __init__(self, *args, **kwargs):
        """Issue warning if dataset does not start with '_'."""
        super().__init__(*args, **kwargs)
        if not self.dataset_id.startswith("_"):
            warnings.warn(
                f"temp dataset {self.dataset_id!r} doesn't start with _"
                ", web console will not consider resulting tables temporary"
            )

    def temp_table(self) -> bigquery.TableReference:
        """Generate a temporary table and return the specified date partition.

        Generates a table name that looks similar to, but won't collide with, a
        server assigned table and that the web console will consider temporary.

        In order for query results to use time partitioning, clustering, or an
        expiration other than 24 hours, destination table must be explicitly set.
        Destination must be generated locally and never collide with server
        assigned table names, because server-assigned tables cannot be modified.
        Server assigned tables for a dry_run query cannot be reused as that
        constitutes a modification. Table expiration can't be set in the query job
        config, but it can be set via a CREATE TABLE statement.

        Server assigned tables have names that start with "anon" and follow with
        either 40 hex characters or a uuid replacing "-" with "_", and cannot be
        modified (i.e. reused).

        The web console considers a table temporary if the dataset name starts with
        "_" and table_id starts with "anon" and is followed by at least one
        character.
        """
        return self.table(f"anon{uuid4().hex}")
