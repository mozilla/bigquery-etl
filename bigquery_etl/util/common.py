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
from typing import List, Optional, Set, Tuple
from uuid import uuid4

import click
import sqlglot
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
            os.path.join(sql_dir, project_dir)
            for project_dir in os.listdir(sql_dir)
            # sql/ can contain files like bigconfig.yml
            if os.path.isdir(os.path.join(sql_dir, project_dir))
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


def qualify_table_references_in_file(path: Path) -> str:
    """Add project id and dataset id to table/view references and persistent udfs in a given query.

    e.g.:
    `table` -> `target_project.default_dataset.table`
    `dataset.table` -> `target_project.dataset.table`

    This allows a query to run in a different project than the sql dir it is located in
    while referencing the same tables.
    """
    # sqlglot cannot handle scripts with variables and control statements
    if re.search(
        r"^\s*DECLARE\b", path.read_text(), flags=re.MULTILINE
    ) or path.name in ("script.sql", "udf.sql"):
        raise NotImplementedError(
            "Cannot qualify table_references of query scripts or UDFs"
        )

    # determine the default target project and dataset from the path
    target_project = Path(path).parent.parent.parent.name
    default_dataset = Path(path).parent.parent.name

    # sqlglot doesn't support Jinja, so we need to render the queries and
    # init queries to raw SQL
    sql_query = render(
        path.name,
        template_folder=path.parent,
        format=False,
        **DEFAULT_QUERY_TEMPLATE_VARS,
    )
    init_query = render(
        path.name,
        template_folder=path.parent,
        format=False,
        is_init=lambda: True,
        metrics=MetricHub(),
    )
    # use sqlglot to get the SQL AST
    init_query_statements = sqlglot.parse(
        init_query,
        read="bigquery",
    )
    sql_query_statements = sqlglot.parse(sql_query, read="bigquery")

    # tuples of (table identifier, replacement string)
    table_replacements: Set[Tuple[str, str]] = set()

    # find all non-fully qualified table/view references including backticks
    for query in [init_query_statements, sql_query_statements]:
        for statement in query:
            if statement is None:
                continue

            cte_names = {
                cte.alias_or_name.lower() for cte in statement.find_all(sqlglot.exp.CTE)
            }

            table_aliases = {
                cte.alias_or_name.lower()
                for cte in statement.find_all(sqlglot.exp.TableAlias)
            }

            for table_expr in statement.find_all(sqlglot.exp.Table):
                # existing table ref including backticks without alias
                table_expr.set("alias", "")
                reference_string = table_expr.sql(dialect="bigquery")

                matched_cte = [
                    re.match(
                        rf"^{cte}(?![a-zA-Z0-9_])",
                        reference_string.replace("`", "").lower(),
                    )
                    for cte in cte_names.union(table_aliases)
                ]
                if any(matched_cte):
                    continue

                # project id is parsed as the catalog attribute
                # but information_schema region may also be parsed as catalog
                if table_expr.catalog.startswith("region-"):
                    project_name = f"{target_project}.{table_expr.catalog}"
                elif table_expr.catalog == "":  # no project id
                    project_name = target_project
                else:  # project id exists
                    continue

                # fully qualified table ref
                replacement_string = f"`{project_name}.{table_expr.db or default_dataset}.{table_expr.name}`"

                table_replacements.add((reference_string, replacement_string))

    updated_query = path.read_text()

    for identifier, replacement in table_replacements:
        if identifier.count(".") == 0:
            # if no dataset and project, only replace if it follows a FROM, JOIN, or implicit cross join
            regex = (
                r"(?P<from>(FROM|JOIN)\s+)"
                r"(?P<cross_join>[a-zA-Z0-9_`.\-]+\s*,\s*)?"
                rf"{identifier}(?![a-zA-Z0-9_`.])"
            )
            replacement = r"\g<from>\g<cross_join>" + replacement
        else:
            identifier = identifier.replace(".", r"\.")
            # ensure match is against the full identifier and no project id already
            regex = rf"(?<![a-zA-Z0-9_`.]){identifier}(?![a-zA-Z0-9_`.])"

        updated_query = re.sub(
            re.compile(regex),
            replacement,
            updated_query,
        )

    # replace udfs from udf/udf_js that do not have a project qualifier
    regex = r"(?<![a-zA-Z0-9_.])`?(?P<dataset>udf(_js)?)`?\.`?(?P<name>[a-zA-Z0-9_]+)`?"
    updated_query = re.sub(
        re.compile(regex),
        rf"`{target_project}.\g<dataset>.\g<name>`",
        updated_query,
    )

    return updated_query


def extract_last_group_by_from_query(
    sql_path: Optional[Path] = None, sql_text: Optional[str] = None
):
    """Return the list of columns in the latest group by of a query."""
    if not sql_path and not sql_text:
        raise click.ClickException(
            "Extracting GROUP BY from query failed due to sql file"
            " or sql text not available."
        )

    if sql_path:
        try:
            query_text = sql_path.read_text()
        except (FileNotFoundError, OSError):
            raise click.ClickException(f'Failed to read query from: "{sql_path}."')
    else:
        query_text = str(sql_text)
    group_by_list = []

    # Remove single and multi-line comments (/* */), trailing semicolon if present and normalize whitespace.
    query_text = re.sub(r"/\*.*?\*/", "", query_text, flags=re.DOTALL)
    query_text = re.sub(r"--[^\n]*\n", "\n", query_text)
    query_text = re.sub(r"\s+", " ", query_text).strip()
    if query_text.endswith(";"):
        query_text = query_text[:-1].strip()

    last_group_by_original = re.search(
        r"(?i){}(?!.*{})".format(re.escape("GROUP BY"), re.escape("GROUP BY")),
        query_text,
        re.DOTALL,
    )

    if last_group_by_original:
        group_by = query_text[last_group_by_original.end() :].lstrip()
        # Remove parenthesis, closing parenthesis, LIMIT, ORDER BY and text after those. Remove also opening parenthesis.
        group_by = (
            re.sub(r"(?i)[\n\)].*|LIMIT.*|ORDER BY.*", "", group_by)
            .replace("(", "")
            .strip()
        )
        if group_by:
            group_by_list = group_by.replace(" ", "").replace("\n", "").split(",")
    return group_by_list


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
