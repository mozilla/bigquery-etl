"""Build and use query dependency graphs."""

import re
import sys
from glob import glob
from itertools import groupby
from pathlib import Path
from subprocess import CalledProcessError
from typing import Dict, Iterator, List, Tuple

import rich_click as click
import sqlglot
import yaml

from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from bigquery_etl.util.common import render

stable_views = None


def _raw_table_name(table: sqlglot.exp.Table) -> str:
    return (
        table.sql("bigquery", comments=False)
        # remove alias
        .split(" AS ", 1)[0]
        # remove quotes
        .replace("`", "")
    )


def extract_table_references(sql: str) -> List[str]:
    """Return a list of tables referenced in the given SQL."""
    # sqlglot cannot handle scripts with variables and control statements
    if re.search(r"^\s*DECLARE\b", sql, flags=re.MULTILINE):
        return []
    # sqlglot parses UDFs with keyword names incorrectly:
    # https://github.com/tobymao/sqlglot/issues/1535
    sql = re.sub(
        r"\.(range|true|false|null)\(",
        r".\1_(",
        sql,
        flags=re.IGNORECASE,
    )
    # sqlglot doesn't suppport OPTIONS on UDFs
    sql = re.sub(
        r"""OPTIONS\s*\(("([^"]|\\")*"|'([^']|\\')*'|[^)])*\)""",
        "",
        sql,
        flags=re.MULTILINE | re.IGNORECASE,
    )
    # sqlglot doesn't fully support byte strings
    sql = re.sub(
        r"""b(["'])""",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )
    query = sqlglot.parse(sql, read="bigquery")
    creates = set()
    tables = set()
    for statement in query:
        if statement is None:
            continue
        creates |= {
            _raw_table_name(expr.this)
            for expr in statement.find_all(sqlglot.exp.Create)
        }
        tables |= (
            {_raw_table_name(table) for table in statement.find_all(sqlglot.exp.Table)}
            # ignore references created in this query
            - creates
            # ignore CTEs created in this statement
            - {cte.alias_or_name for cte in statement.find_all(sqlglot.exp.CTE)}
        )
    return sorted(tables)


def extract_table_references_without_views(path: Path) -> Iterator[str]:
    """Recursively search for non-view tables referenced in the given SQL file."""
    global stable_views

    sql = render(path.name, template_folder=path.parent)
    for table in extract_table_references(sql):
        ref_base = path.parent
        parts = tuple(table.split("."))
        for _ in parts:
            ref_base = ref_base.parent
        view_paths = [ref_base.joinpath(*parts, "view.sql")]
        if parts[:1] == (
            ConfigLoader.get("default", "user_facing_project", fallback="mozdata"),
        ):
            view_paths.append(
                ref_base.joinpath(
                    ConfigLoader.get(
                        "default", "project", fallback="moz-fx-data-shared-prod"
                    ),
                    *parts[1:],
                    "view.sql",
                ),
            )
        for view_path in view_paths:
            if view_path == path:
                continue  # skip self references
            if view_path.is_file():
                yield from extract_table_references_without_views(view_path)
                break
        else:
            # use directory structure to fully qualify table names
            while len(parts) < 3:
                parts = (ref_base.name, *parts)
                ref_base = ref_base.parent
            if parts[:-2] in (
                (
                    ConfigLoader.get(
                        "default", "project", fallback="moz-fx-data-shared-prod"
                    ),
                ),
                (
                    ConfigLoader.get(
                        "default", "user_facing_project", fallback="mozdata"
                    ),
                ),
            ):
                if stable_views is None:
                    # lazy read stable views
                    stable_views = {
                        tuple(schema.user_facing_view.split(".")): tuple(
                            schema.stable_table.split(".")
                        )
                        for schema in get_stable_table_schemas()
                    }
                if parts[-2:] in stable_views:
                    parts = (
                        ConfigLoader.get(
                            "default", "project", fallback="moz-fx-data-shared-prod"
                        ),
                        *stable_views[parts[-2:]],
                    )
            yield ".".join(parts)


def _get_references(
    paths: Tuple[str, ...], without_views: bool = False
) -> Iterator[Tuple[Path, List[str]]]:
    file_paths = {
        path
        for parent in map(Path, paths or ["sql"])
        for path in (
            map(Path, glob(f"{parent}/**/*.sql", recursive=True))
            if parent.is_dir()
            else [parent]
        )
        if not path.name.endswith(".template.sql")  # skip templates
    }
    fail = False
    for path in sorted(file_paths):
        try:
            if without_views:
                yield path, list(extract_table_references_without_views(path))
            else:
                sql = render(path.name, template_folder=path.parent)
                yield path, extract_table_references(sql)
        except CalledProcessError as e:
            raise click.ClickException(f"failed to import jnius: {e}")
        except ImportError as e:
            raise click.ClickException(*e.args)
        except ValueError as e:
            fail = True
            print(f"Failed to parse {path}: {e}", file=sys.stderr)
    if fail:
        raise click.ClickException("Some paths could not be analyzed")


def get_dependency_graph(
    paths: Tuple[str, ...], without_views: bool = False
) -> Dict[str, List[str]]:
    """Return the query dependency graph."""
    refs = _get_references(paths, without_views=without_views)
    dependency_graph = {}

    for ref in refs:
        table = ref[0].parent.name
        dataset = ref[0].parent.parent.name
        project = ref[0].parent.parent.parent.name
        dependency_graph[f"{project}.{dataset}.{table}"] = ref[1]

    return dependency_graph


@click.group(help=__doc__)
def dependency():
    """Create the CLI group for dependency commands."""
    pass


@dependency.command(
    help="Show table references in sql files.",
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(file_okay=True),
)
@click.option(
    "--without-views",
    "--without_views",
    is_flag=True,
    help="recursively resolve view references to underlying tables",
)
def show(paths: Tuple[str, ...], without_views: bool):
    """Show table references in sql files."""
    for path, table_references in _get_references(paths, without_views):
        if table_references:
            for table in table_references:
                print(f"{path}: {table}")
        else:
            print(f"{path} contains no table references", file=sys.stderr)


@dependency.command(
    help="Record table references in metadata. Fails if metadata already contains "
    "references section.",
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(file_okay=True),
)
@click.option(
    "--skip-existing",
    "--skip_existing",
    is_flag=True,
    help="Skip files with existing references rather than failing",
)
def record(paths: Tuple[str, ...], skip_existing):
    """Record table references in metadata."""
    for parent, group in groupby(_get_references(paths), lambda e: e[0].parent):
        references = {
            path.name: table_references
            for path, table_references in group
            if table_references
        }
        if not references:
            continue
        with open(parent / "metadata.yaml", "a+") as f:
            f.seek(0)
            metadata = yaml.safe_load(f)
            if metadata is None:
                pass  # new or empty metadata.yaml
            elif not isinstance(metadata, dict):
                raise click.ClickException(f"{f.name} is not valid metadata")
            elif "references" in metadata:
                if skip_existing:
                    # Continue without modifying metadata.yaml
                    continue
                raise click.ClickException(f"{f.name} already contains references")
            f.write("\n# Generated by bigquery_etl.dependency\n")
            f.write(yaml.dump({"references": references}))
