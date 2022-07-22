"""Generic utility functions."""
import logging
import os
import random
import re
import string
import logging
import warnings
from typing import List
from pathlib import Path
from uuid import uuid4

from google.cloud import bigquery

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.format_sql.formatter import reformat

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
SQL_DIR = "sql/"
FILE_PATH = Path(os.path.dirname(__file__))


def snake_case(line: str) -> str:
    """Convert a string into a snake_cased string."""
    # replace non-alphanumeric characters with spaces in the reversed line
    subbed = re.sub(r"[^\w]|_", " ", line[::-1])
    # apply the regex on the reversed string
    words = REV_WORD_BOUND_PAT.split(subbed)
    # filter spaces between words and snake_case and reverse again
    return "_".join([w.lower() for w in words if w.strip()])[::-1]


def project_dirs(project_id=None) -> List[str]:
    """Return all project directories."""
    if project_id is None:
        return [
            os.path.join(SQL_DIR, project_dir) for project_dir in os.listdir(SQL_DIR)
        ]
    else:
        return [os.path.join(SQL_DIR, project_id)]


def random_str(length: int = 12) -> str:
    """Return a random string of the specified length."""
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def render(sql_filename, format=True, template_folder="glean_usage", **kwargs) -> str:
    """Render a given template query using Jinja."""
    file_loader = FileSystemLoader(f"{template_folder}/templates")
    env = Environment(loader=file_loader)
    main_sql = env.get_template(sql_filename)
    rendered = main_sql.render(**kwargs)
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
