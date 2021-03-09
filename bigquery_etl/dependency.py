"""Build and use query dependency graphs."""

import sys
from itertools import groupby
from pathlib import Path
from subprocess import CalledProcessError
from typing import Iterator, List, Tuple

import click
import jnius_config
import yaml

from .view.generate_stable_views import get_stable_table_schemas

# this has to run before jnius can be imported
for path in (Path(__file__).parent.parent / "target" / "dependency").glob("*.jar"):
    jnius_config.add_classpath(path.resolve().as_posix())

stable_views = None


def extract_table_references(sql: str) -> List[str]:
    """Return a list of tables referenced in the given SQL."""
    # import jnius here so this module can be imported safely without java installed
    import jnius  # noqa: E402

    try:
        Analyzer = jnius.autoclass("com.google.zetasql.Analyzer")
        AnalyzerOptions = jnius.autoclass("com.google.zetasql.AnalyzerOptions")
    except jnius.JavaException:
        # replace jnius.JavaException because it's not available outside this function
        raise ImportError(
            "failed to import java class via jni, please download java dependencies "
            "with: mvn dependency:copy-dependencies"
        )
    # enable support for CreateViewStatement and others
    options = AnalyzerOptions()
    options.getLanguageOptions().setSupportsAllStatementKinds()
    try:
        result = Analyzer.extractTableNamesFromStatement(sql, options)
    except jnius.JavaException:
        # Only use extractTableNamesFromScript when extractTableNamesFromStatement
        # fails, because for scripts zetasql incorrectly includes CTE references from
        # subquery expressions
        try:
            result = Analyzer.extractTableNamesFromScript(sql, options)
        except jnius.JavaException as e:
            # replace jnius.JavaException because it's not available outside this function
            raise ValueError(*e.args)
    return [".".join(table.toArray()) for table in result.toArray()]


def extract_table_references_without_views(path: Path) -> Iterator[str]:
    """Recursively search for non-view tables referenced in the given SQL file."""
    global stable_views

    for table in extract_table_references(path.read_text()):
        ref_base = path.parent
        parts = tuple(table.split("."))
        for _ in parts:
            ref_base = ref_base.parent
        view_path = ref_base.joinpath(*parts, "view.sql")
        if view_path.is_file():
            yield from extract_table_references_without_views(view_path)
        else:
            # use directory structure to fully qualify table names
            while len(parts) < 3:
                parts = (ref_base.name, *parts)
                ref_base = ref_base.parent
            if parts[:-2] == ("moz-fx-data-shared-prod",):
                if stable_views is None:
                    # lazy read stable views
                    stable_views = {
                        tuple(schema.user_facing_view.split(".")): tuple(
                            schema.stable_table.split(".")
                        )
                        for schema in get_stable_table_schemas()
                    }
                if parts[-2:] in stable_views:
                    parts = ("moz-fx-data-shared-prod", *stable_views[parts[-2:]])
            yield ".".join(parts)


def _get_references(
    paths: Tuple[str, ...], without_views: bool = False
) -> Iterator[Tuple[Path, List[str]]]:
    file_paths = {
        path
        for parent in map(Path, paths or ["sql"])
        for path in (parent.glob("**/*.sql") if parent.is_dir() else [parent])
        if not path.name.endswith(".template.sql")  # skip templates
    }
    fail = False
    for path in sorted(file_paths):
        try:
            if without_views:
                yield path, list(extract_table_references_without_views(path))
            else:
                yield path, extract_table_references(path.read_text())
        except CalledProcessError as e:
            raise click.ClickException(f"failed to import jnius: {e}")
        except ImportError as e:
            raise click.ClickException(*e.args)
        except ValueError as e:
            fail = True
            print(f"Failed to parse {path}: {e}", file=sys.stderr)
    if fail:
        raise click.ClickException("Some paths could not be analyzed")


@click.group(help=__doc__)
def dependency():
    """Create the CLI group for dependency commands."""
    pass


@dependency.command(
    help="Show table references in sql files. Requires Java.",
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
    "references section. Requires Java.",
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(file_okay=True),
)
def record(paths: Tuple[str, ...]):
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
                raise click.ClickException(f"{f.name} already contains references")
            f.write("\n# Generated by bigquery_etl.dependency\n")
            f.write(yaml.dump({"references": references}))
