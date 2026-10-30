"""Copy checked-in SQL on top of previously generated SQL.

CI skips the SQL generators when a PR can't affect their output and instead copies
the PR's sql/ directory over the published generated-sql tree. A plain copy breaks
for generated views that have a checked-in metadata.yaml holding only overrides
(e.g. workgroup_access for fenix.crash). The generators merge those overrides into
the generated metadata, so copying the checked-in file over it drops the generated
friendly_name, description and labels.

Overrides the PR didn't change are already merged into the generated file, so it's
kept as is. Overrides the PR did change need the generators to run, which CI
detects with `changed-overrides` before deciding whether to regenerate.
"""

import filecmp
import shutil
import sys
from pathlib import Path

import click
import yaml

from bigquery_etl.metadata.parse_metadata import METADATA_FILE


class ChangedOverrideError(Exception):
    """Raised when overlaying an override that requires running the generators."""


def _is_generated_override(src_file: Path, generated_file: Path) -> bool:
    """Return whether src_file is a partial override of generated metadata.

    Uses the condition glean_app_ping_views checks before merging existing metadata.
    """
    if src_file.name != METADATA_FILE:
        return False
    if not generated_file.is_file() or generated_file.is_symlink():
        return False
    generated = yaml.safe_load(generated_file.read_text()) or {}
    if "sql_generator" not in (generated.get("labels") or {}):
        return False
    checked_in = yaml.safe_load(src_file.read_text()) or {}
    return "friendly_name" not in checked_in and "description" not in checked_in


def _is_unchanged(src_file: Path, base_file: Path) -> bool:
    return base_file.is_file() and filecmp.cmp(src_file, base_file, shallow=False)


def changed_overrides(src: Path, generated: Path, base: Path) -> list[Path]:
    """Return overrides of generated metadata in src that differ from base.

    base is the checked-in sql directory the generated tree was built from.
    """
    return sorted(
        src_file
        for src_file in src.rglob(METADATA_FILE)
        if _is_generated_override(src_file, generated / src_file.relative_to(src))
        and not _is_unchanged(src_file, base / src_file.relative_to(src))
    )


def overlay_sql_dir(src: Path, dest: Path, base: Path) -> None:
    """Copy src over dest, keeping generated metadata for unchanged overrides.

    base is the checked-in sql directory the generated tree in dest was built from.
    Raises ChangedOverrideError if src changes an override, since only the
    generators can merge it correctly.
    """
    src, dest, base = Path(src), Path(dest), Path(base)

    def _copy(src_file, dest_file):
        src_file, dest_file = Path(src_file), Path(dest_file)
        if _is_generated_override(src_file, dest_file):
            if not _is_unchanged(src_file, base / src_file.relative_to(src)):
                raise ChangedOverrideError(
                    f"{src_file} overrides generated metadata and has changed, "
                    "run the SQL generators instead"
                )
            return dest_file
        return shutil.copy2(src_file, dest_file)

    shutil.copytree(src, dest, symlinks=True, dirs_exist_ok=True, copy_function=_copy)


_base_option = click.option(
    "--base",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Checked-in sql directory the generated SQL was built from",
)


@click.group()
def main():
    """Overlay checked-in SQL onto generated SQL."""


@main.command("copy")
@click.argument("src", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("dest", type=click.Path(exists=True, file_okay=False, path_type=Path))
@_base_option
def copy(src, dest, base):
    """Copy checked-in SQL in SRC over generated SQL in DEST."""
    overlay_sql_dir(src, dest, base)


@main.command("changed-overrides")
@click.argument("src", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument(
    "generated", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@_base_option
def changed_overrides_command(src, generated, base):
    """Print overrides of generated metadata in SRC that changed since BASE."""
    for path in changed_overrides(src, generated, base):
        click.echo(path)


if __name__ == "__main__":
    sys.exit(main())
