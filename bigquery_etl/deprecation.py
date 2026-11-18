"""Select downstream assets to deprecate along with a table."""

import subprocess
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any, Sequence

import attrs
import click

from bigquery_etl.config import ConfigLoader
from bigquery_etl.datahub.lineage import (
    DATAHUB_LINEAGE_DEGREES,
    last_query_text,
    sql_path,
    type_label,
    type_rank,
)
from bigquery_etl.dependency import extract_table_references
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata

# Read-only picker groups show at most this many rows.
MAX_READ_ONLY_ROWS = 25
# DataHub counts queries over this many days. Tables added to git more recently
# than that are never treated as unused.
USAGE_WINDOW_DAYS = 30
# prompt_toolkit styles of the picker. Only selectable rows are colored.
PICKER_STYLE = [
    ("separator", "bold"),
    ("pointer", "bold"),
    ("selected", "fg:ansigreen bold"),
    ("disabled", "fg:default nobold noitalic"),
    ("instruction", "fg:ansibrightblack"),
]
# Row color of selectable assets, by degree_label.
DEGREE_STYLES = {"1": "fg:ansicyan", "2": "fg:ansiyellow", "3+": "fg:ansimagenta"}
# Color of the platform name in read-only rows, as in PLATFORM_COLORS.
PLATFORM_STYLES = {"redash": "fg:#ff8700", "looker": "fg:ansigreen"}


class IncludeDownstream(StrEnum):
    """Which downstream assets --include-downstream pre-selects."""

    ALL = "all"
    UNUSED = "unused"  # unqueried and older than USAGE_WINDOW_DAYS
    NONE = "none"


class DisabledReason(Enum):
    """Why a downstream directory can't be selected, in picker order."""

    ALREADY_DEPRECATED = "already deprecated"
    # Either generated SQL, or a new table that hasn't been added to git yet.
    UNTRACKED = "not tracked by git (generated SQL, or new and needs `git add`)"
    GIT_UNAVAILABLE = "can't check git for generated SQL"
    INVALID_METADATA = "invalid metadata.yaml"
    NO_METADATA = "no metadata.yaml"


@attrs.define
class Candidate:
    """A downstream table or view directory in bigquery-etl."""

    path: Path
    assets: list[dict[str, Any]]
    disabled_reason: DisabledReason | None = None
    # Added to git in the last USAGE_WINDOW_DAYS days.
    is_new: bool = False
    # Downstream views whose SQL reads from this table or view.
    dependent_views: list["Candidate"] = attrs.field(factory=list)

    @property
    def name(self) -> str:
        """Return project.dataset.table of the directory."""
        return ".".join(self.path.parts[-3:])

    @property
    def degree(self) -> int:
        """Return the closest degree of any asset defined in this directory."""
        return min(a["degree"] for a in self.assets)

    @property
    def queries_30d(self) -> int | None:
        """Return total 30-day queries, or None if no asset has usage data."""
        counts = [a["queries_30d"] for a in self.assets if a["queries_30d"] is not None]
        return sum(counts) if counts else None

    @property
    def last_query(self) -> str:
        """Return the latest query date across assets, as last_query_text shows it."""
        dates = [a["last_query"] for a in self.assets if a["last_query"]]
        return last_query_text(max(dates, default=None), self.queries_30d)

    @property
    def subtype(self) -> str:
        """Return the DataHub subtype of the first asset, e.g. Table or View."""
        return self.assets[0]["subtype"] or "Dataset"


def downstream_candidates(
    assets: list[dict[str, Any]], sql_dir: str, exclude: set[Path]
) -> tuple[list[Candidate], list[dict[str, Any]]]:
    """Split downstream assets into bigquery-etl directories and everything else.

    Assets defined in an excluded directory (the tables being deprecated) are dropped.
    """
    exclude = {p.resolve() for p in exclude}
    by_path: dict[Path, list[dict[str, Any]]] = {}
    external = []
    for a in assets:
        path = sql_path(a, sql_dir)
        if path is None:
            external.append(a)
        elif path.resolve() not in exclude:
            by_path.setdefault(path, []).append(a)

    tracked = tracked_files(sql_dir) if by_path else set()
    if tracked is None:
        click.echo(
            f"Warning: could not list git-tracked files in {sql_dir}, so generated "
            "SQL can't be told apart and no downstream can be selected.",
            err=True,
        )
    recent = recently_added(sql_dir) if tracked else set()
    candidates = []
    for path, path_assets in by_path.items():
        candidate = Candidate(path, path_assets)
        metadata_file = path / METADATA_FILE
        if not metadata_file.exists():
            candidate.disabled_reason = DisabledReason.NO_METADATA
        elif tracked is None:
            candidate.disabled_reason = DisabledReason.GIT_UNAVAILABLE
        elif metadata_file.resolve() not in tracked:
            candidate.disabled_reason = DisabledReason.UNTRACKED
        else:
            try:
                if Metadata.from_file(metadata_file).deprecated:
                    candidate.disabled_reason = DisabledReason.ALREADY_DEPRECATED
            except Exception:
                candidate.disabled_reason = DisabledReason.INVALID_METADATA
        # Without git history the age is unknown, which counts as new.
        candidate.is_new = recent is None or metadata_file.resolve() in recent
        candidates.append(candidate)
    candidates.sort(key=lambda c: (c.degree, c.name))
    link_views(candidates)
    return candidates, external


def link_views(candidates: list[Candidate]) -> None:
    """Fill in Candidate.dependent_views by reading each candidate view's SQL."""
    user_facing = ConfigLoader.get("default", "user_facing_project", fallback="mozdata")
    default = ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
    by_name = {c.name: c for c in candidates}
    for view in candidates:
        view_file = view.path / "view.sql"
        if not view_file.exists():
            continue
        try:
            references = extract_table_references(view_file.read_text())
        except Exception:  # e.g. templated SQL that can't be parsed
            continue
        for reference in references:
            parts = reference.split(".")
            if len(parts) == 2:
                parts.insert(0, view.path.parts[-3])
            if parts[0] == user_facing:
                parts[0] = default
            source = by_name.get(".".join(parts))
            if source is not None and source is not view:
                source.dependent_views.append(view)


def _git_metadata_files(sql_dir: str, *command: str) -> set[Path] | None:
    """Return the metadata files a git command lists, or None if git fails."""
    root = Path(sql_dir).resolve()
    try:
        result = subprocess.run(
            ["git", *command, "-z", "--", f"*/{METADATA_FILE}"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    files = (f.strip() for f in result.stdout.split("\0"))
    return {(root / f).resolve() for f in files if f}


def tracked_files(sql_dir: str) -> set[Path] | None:
    """Return the metadata files git tracks in sql_dir, or None if git fails."""
    return _git_metadata_files(sql_dir, "ls-files")


def recently_added(sql_dir: str) -> set[Path] | None:
    """Return the metadata files first committed within USAGE_WINDOW_DAYS."""
    return _git_metadata_files(
        sql_dir,
        "log",
        f"--since={USAGE_WINDOW_DAYS}.days",
        "--diff-filter=A",
        "--name-only",
        "--relative",
        "--format=",
    )


def used_reason(candidate: Candidate) -> str | None:
    """Return why a candidate counts as used, or None if it is unused.

    No usage data counts as no queries. A candidate is used if any view of it is.
    """
    if candidate.queries_30d:
        noun = "query" if candidate.queries_30d == 1 else "queries"
        return f"{candidate.queries_30d} {noun} in {USAGE_WINDOW_DAYS} days"
    if candidate.is_new:
        return f"added in the last {USAGE_WINDOW_DAYS} days"
    for view in candidate.dependent_views:
        reason = used_reason(view)
        if reason:
            return f"via view {view.name}: {reason}"
    return None


def is_unused(candidate: Candidate) -> bool:
    """Return True if nothing queried it, or a view of it, in the usage window."""
    return used_reason(candidate) is None


def preselect(candidates: list[Candidate], mode: IncludeDownstream) -> set[Path]:
    """Return the directories to pre-check for an --include-downstream mode."""
    selectable = [c for c in candidates if not c.disabled_reason]
    if mode == IncludeDownstream.ALL:
        return {c.path for c in selectable}
    if mode == IncludeDownstream.UNUSED:
        return {c.path for c in selectable if is_unused(c)}
    return set()


def selection_summary(
    candidates: list[Candidate], selected: set[Path], mode: IncludeDownstream
) -> str:
    """List which downstream directories are deprecated and why others aren't."""
    chosen = [c for c in candidates if c.path in selected]
    skipped = [c for c in candidates if c.path not in selected]
    lines = [f"Deprecating {len(chosen)} downstream:"]
    lines += [f"  {c.name}" for c in chosen]
    if skipped:
        lines.append(f"Not deprecating {len(skipped)} downstream:")
    for c in skipped:
        if c.disabled_reason:
            reason = c.disabled_reason.value
        else:
            reason = (
                used_reason(c) if mode == IncludeDownstream.UNUSED else None
            ) or "not selected"
        lines.append(f"  {c.name} ({reason})")
    return "\n".join(lines)


def degree_label(degree: int) -> str:
    """Return the degree as DataHub's facet shows it (1, 2, 3+)."""
    return DATAHUB_LINEAGE_DEGREES[min(degree, len(DATAHUB_LINEAGE_DEGREES)) - 1]


def candidate_label(candidate: Candidate, width: int) -> str:
    """Return a checkbox row for a bigquery-etl directory."""
    queries = "n/a" if candidate.queries_30d is None else candidate.queries_30d
    # Say why a row with no queries of its own still isn't unused.
    note = "" if candidate.queries_30d else used_reason(candidate) or ""
    return (
        f"{candidate.name.ljust(width)}  {candidate.subtype:<7}  "
        f"{queries:>11}  {candidate.last_query:<10}  {note}"
    ).rstrip()


def candidate_columns(width: int) -> str:
    """Return the column header of candidate_label rows."""
    return f"{'NAME'.ljust(width)}  {'TYPE':<7}  {'QUERIES 30D':>11}  LAST QUERY  NOTE"


EXTERNAL_COLUMNS = f"{'DEGREE':>6}  {'PLATFORM':<8}  {'TYPE':<10}  NAME"

# A row label: plain text, or prompt_toolkit (style, text) fragments.
Label = str | list[tuple[str, str]]
Rows = list[tuple[Label, Path | None]]


def external_label(asset: dict[str, Any]) -> Label:
    """Return a read-only row for an asset outside bigquery-etl."""
    platform = asset["platform"] or "unknown"
    return [
        ("", f"{degree_label(asset['degree']):>6}  "),
        (PLATFORM_STYLES.get(platform, ""), f"{platform:<8}"),
        ("", f"  {type_label(asset):<10}  {asset['qualified_name']}"),
    ]


def label_text(label: Label) -> str:
    """Return a row label without styling."""
    return label if isinstance(label, str) else "".join(text for _, text in label)


@attrs.define
class Section:
    """A titled group of picker rows; style colors its selectable rows."""

    title: str
    columns: str
    rows: Rows
    style: str = ""


def picker_sections(
    candidates: list[Candidate],
    external: list[dict[str, Any]],
) -> list[Section]:
    """Return picker sections; a row is (label, path or None if read-only)."""
    selectable = [c for c in candidates if not c.disabled_reason]
    width = max([len(c.name) for c in candidates] + [len("NAME")])
    columns = candidate_columns(width)
    sections: list[Section] = []

    degrees = sorted({degree_label(c.degree) for c in selectable})
    for degree in degrees:
        rows = [c for c in selectable if degree_label(c.degree) == degree]
        sections.append(
            Section(
                f"Degree {degree} ({len(rows)})",
                columns,
                [(candidate_label(c, width), c.path) for c in rows],
                DEGREE_STYLES[degree],
            )
        )

    for reason in DisabledReason:
        rows = [c for c in candidates if c.disabled_reason == reason]
        if rows:
            sections.append(
                Section(
                    f"Can't deprecate: {reason.value} ({len(rows)})",
                    columns,
                    _capped([candidate_label(c, width) for c in rows]),
                )
            )

    platforms: dict[str, list[dict[str, Any]]] = {}
    for a in external:
        platforms.setdefault(a["platform"] or "unknown", []).append(a)
    for platform in sorted(platforms):
        platform_assets = sorted(
            platforms[platform],
            key=lambda a: (a["degree"], type_rank(a["type"]), a["qualified_name"]),
        )
        sections.append(
            Section(
                f"Not in bigquery-etl: {platform} ({len(platform_assets)})",
                EXTERNAL_COLUMNS,
                _capped([external_label(a) for a in platform_assets]),
            )
        )
    return sections


def _capped(labels: Sequence[Label]) -> Rows:
    """Return read-only rows, truncated with a summary row."""
    rows: Rows = [(label, None) for label in labels[:MAX_READ_ONLY_ROWS]]
    if len(labels) > MAX_READ_ONLY_ROWS:
        rows.append(
            (
                f"... and {len(labels) - MAX_READ_ONLY_ROWS} more "
                "(see ./bqetl metadata get-downstream-lineage)",
                None,
            )
        )
    return rows


def pick(
    targets: str,
    candidates: list[Candidate],
    external: list[dict[str, Any]],
    preselected: set[Path],
    note: str = "",
) -> list[Path] | None:
    """Ask which downstream directories to deprecate; note is appended to the title.

    Returns the selected directories, [] to skip downstream, or None to cancel.
    """
    import questionary

    sections = picker_sections(candidates, external)
    if not any(path for section in sections for _, path in section.rows):
        click.echo(f"No downstream assets of {targets}{note} can be deprecated here.")
        for section in sections:
            click.echo(click.style(f"── {section.title} ──", bold=True))
            click.echo(click.style(f"  {section.columns}", bold=True))
            for label, _ in section.rows:
                click.echo(f"  {label_text(label)}")
        return [] if click.confirm(f"Deprecate {targets}?", default=True) else None

    choices: list[Any] = []
    for section in sections:
        choices.append(questionary.Separator(f"── {section.title} ──"))
        # Rows start two characters in, after their "○ " or "- " marker.
        choices.append(questionary.Separator(f"  {section.columns}"))
        for label, path in section.rows:
            if path is None:
                choices.append(questionary.Choice(label, disabled=True))  # type: ignore[arg-type]
            else:
                choices.append(
                    questionary.Choice(
                        [(section.style, label_text(label))],
                        value=path,
                        checked=path in preselected,
                    )
                )
    return questionary.checkbox(
        f"Downstream of {targets}{note}. Select other assets to deprecate:",
        choices=choices,
        instruction="(<space> to select, <a> to toggle all, <i> to invert, "
        f"<enter> to deprecate {targets} and the selection, <ctrl-c> to cancel)",
        style=questionary.Style(PICKER_STYLE),
    ).ask()
