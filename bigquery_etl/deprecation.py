"""Select downstream assets to deprecate along with a table."""

import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional, Sequence

import attrs
import click

from bigquery_etl.datahub.lineage import DATAHUB_LINEAGE_DEGREES, sql_path, type_rank
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata

INCLUDE_DOWNSTREAM_MODES = ["all", "unused", "none"]
# Read-only picker groups show at most this many rows.
MAX_READ_ONLY_ROWS = 25
# Usage counts cover this many days, so newer tables can't be called unused.
USAGE_WINDOW_DAYS = 30
ALREADY_DEPRECATED = "already deprecated"
NO_METADATA = "no metadata.yaml"
INVALID_METADATA = "invalid metadata.yaml"
# Generated SQL isn't committed, but neither is a new table before `git add`.
UNTRACKED = "not tracked by git (generated SQL, or new and needs `git add`)"
GIT_UNAVAILABLE = "can't check git for generated SQL"
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
DISABLED_REASONS = (
    ALREADY_DEPRECATED,
    UNTRACKED,
    GIT_UNAVAILABLE,
    INVALID_METADATA,
    NO_METADATA,
)


@attrs.define
class Candidate:
    """A downstream table or view directory in bigquery-etl."""

    path: Path
    assets: list[dict[str, Any]]
    disabled_reason: Optional[str] = None

    @property
    def name(self) -> str:
        """Return project.dataset.table of the directory."""
        return ".".join(self.path.parts[-3:])

    @property
    def degree(self) -> int:
        """Return the closest degree of any asset defined in this directory."""
        return min(a["degree"] for a in self.assets)

    @property
    def queries_30d(self) -> Optional[int]:
        """Return total 30-day queries, or None if any asset lacks usage data."""
        counts = [a["queries_30d"] for a in self.assets]
        return None if None in counts else sum(counts)

    @property
    def last_query(self) -> str:
        """Return the latest query date across assets, or none / n/a."""
        dates = [a["last_query"] for a in self.assets if a["last_query"]]
        if dates:
            return max(dates)
        return "none" if self.queries_30d == 0 else "n/a"

    @property
    def created(self) -> Optional[str]:
        """Return the latest creation date across assets, or None if any is unknown."""
        dates = [a["created"] for a in self.assets]
        return None if None in dates else max(dates)

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
    candidates = []
    for path, path_assets in by_path.items():
        candidate = Candidate(path, path_assets)
        metadata_file = path / METADATA_FILE
        if not metadata_file.exists():
            candidate.disabled_reason = NO_METADATA
        elif tracked is None:
            candidate.disabled_reason = GIT_UNAVAILABLE
        elif metadata_file.resolve() not in tracked:
            candidate.disabled_reason = UNTRACKED
        else:
            try:
                if Metadata.from_file(metadata_file).deprecated:
                    candidate.disabled_reason = ALREADY_DEPRECATED
            except Exception:
                candidate.disabled_reason = INVALID_METADATA
        candidates.append(candidate)
    candidates.sort(key=lambda c: (c.degree, c.name))
    return candidates, external


def tracked_files(sql_dir: str) -> Optional[set[Path]]:
    """Return the metadata files git tracks in sql_dir; None if git can't tell."""
    root = Path(sql_dir).resolve()
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z", "--", f"*/{METADATA_FILE}"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return {(root / f).resolve() for f in result.stdout.split("\0") if f}


def used_reason(candidate: Candidate) -> Optional[str]:
    """Return why a candidate isn't unused; unknown usage or age counts as used."""
    if candidate.queries_30d is None:
        return "usage unknown"
    if candidate.queries_30d:
        return f"{candidate.queries_30d} queries in {USAGE_WINDOW_DAYS} days"
    if candidate.created is None:
        return "creation date unknown"
    cutoff = date.today() - timedelta(days=USAGE_WINDOW_DAYS)
    if date.fromisoformat(candidate.created) > cutoff:
        return f"created in the last {USAGE_WINDOW_DAYS} days"
    return None


def is_unused(candidate: Candidate) -> bool:
    """Return True if unqueried for 30 days and older than that."""
    return used_reason(candidate) is None


def preselect(candidates: list[Candidate], mode: str) -> set[Path]:
    """Return the directories to pre-check for an --include-downstream mode."""
    selectable = [c for c in candidates if not c.disabled_reason]
    if mode == "all":
        return {c.path for c in selectable}
    if mode == "unused":
        return {c.path for c in selectable if is_unused(c)}
    return set()


def selection_summary(
    candidates: list[Candidate], selected: set[Path], mode: str
) -> str:
    """List which downstream directories are deprecated and why others aren't."""
    chosen = [c for c in candidates if c.path in selected]
    skipped = [c for c in candidates if c.path not in selected]
    lines = [f"Deprecating {len(chosen)} downstream:"]
    lines += [f"  {c.name}" for c in chosen]
    if skipped:
        lines.append(f"Not deprecating {len(skipped)} downstream:")
    for c in skipped:
        reason = c.disabled_reason or (
            used_reason(c) if mode == "unused" else "not selected"
        )
        lines.append(f"  {c.name} ({reason})")
    return "\n".join(lines)


def degree_label(degree: int) -> str:
    """Return the degree as DataHub's facet shows it (1, 2, 3+)."""
    return DATAHUB_LINEAGE_DEGREES[min(degree, len(DATAHUB_LINEAGE_DEGREES)) - 1]


def candidate_label(candidate: Candidate, width: int) -> str:
    """Return a checkbox row for a bigquery-etl directory."""
    queries = "n/a" if candidate.queries_30d is None else candidate.queries_30d
    return (
        f"{candidate.name.ljust(width)}  {candidate.subtype:<7}  "
        f"{queries:>11}  {candidate.last_query}"
    )


def candidate_columns(width: int) -> str:
    """Return the column header of candidate_label rows."""
    return f"{'NAME'.ljust(width)}  {'TYPE':<7}  {'QUERIES 30D':>11}  LAST QUERY"


EXTERNAL_COLUMNS = f"{'DEGREE':>6}  {'PLATFORM':<8}  {'TYPE':<10}  NAME"

# A row label: plain text, or prompt_toolkit (style, text) fragments.
Label = str | list[tuple[str, str]]
Rows = list[tuple[Label, Optional[Path]]]


def external_label(asset: dict[str, Any]) -> Label:
    """Return a read-only row for an asset outside bigquery-etl."""
    platform = asset["platform"] or "unknown"
    type_ = asset["subtype"] or asset["type"].replace("_", " ").capitalize()
    return [
        ("", f"{degree_label(asset['degree']):>6}  "),
        (PLATFORM_STYLES.get(platform, ""), f"{platform:<8}"),
        ("", f"  {type_:<10}  {asset['qualified_name']}"),
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

    for reason in DISABLED_REASONS:
        rows = [c for c in candidates if c.disabled_reason == reason]
        if rows:
            sections.append(
                Section(
                    f"Can't deprecate: {reason} ({len(rows)})",
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
) -> Optional[list[Path]]:
    """Ask which downstream directories to deprecate.

    note describes the options the lineage was fetched with, for the title.

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
