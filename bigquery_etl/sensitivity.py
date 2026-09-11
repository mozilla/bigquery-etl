"""Flag risky data flows in SQL queries.

Detects when a query reads *sensitive* (restricted / narrowly workgroup-gated)
data and writes it to a *more broadly readable* destination.

Only handles SQL (`query.sql`); `query.py` builds table names dynamically and
can't be resolved statically.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pathspec

from bigquery_etl.config import ConfigLoader
from bigquery_etl.dependency import extract_table_references
from bigquery_etl.metadata.parse_metadata import (
    DATASET_METADATA_FILE,
    METADATA_FILE,
    DatasetMetadata,
    Metadata,
)
from bigquery_etl.util.common import render


def _config(key: str, default):
    """Read a `sensitivity.<key>` value from bqetl_project.yaml, or `default`."""
    value = ConfigLoader.get("sensitivity", key, fallback=None)
    return value if value is not None else default


# roles that grant the ability to read row-level data
READ_ROLES = set(_config("read_roles", ["roles/bigquery.dataViewer"]))
# the org-wide "everyone with confidential access" reader; anything scoped only
# to this (or broader) is not considered sensitive
BROAD_READERS = set(
    _config("broad_readers", ["workgroup:mozilla-confidential/data-viewers"])
)
# projects whose datasets are public/non-sensitive and safe to read from.
# mozfun is the shared public UDF library; every other unresolved source is
# treated as sensitive (can't be verified) rather than skipped.
SAFE_PROJECTS = set(_config("safe_projects", ["mozfun"]))
# stable/live dataset suffixes: unless listed in `gated_datasets`, a stable/live
# dataset in an ingestion project is granted the broad default and treated as
# non-sensitive.
INGESTION_SUFFIXES = tuple(_config("ingestion_suffixes", ["_stable", "_live"]))
# CODEOWNERS file used to decide whether a flagged flow is already owned/reviewed.
CODEOWNERS_FILE = _config("codeowners_file", "CODEOWNERS")
# version suffix on stable/live table names (e.g. main_v5); gated_datasets keys
# tables by the version-stripped doctype.
_VERSION_RE = re.compile(r"_v[0-9]+$")


def load_gated_datasets() -> Dict[str, Dict]:
    """Load the `sensitivity.gated_datasets` map (keyed by project, then dataset)."""
    return ConfigLoader.get("sensitivity", "gated_datasets", fallback={}) or {}


def gated_readers(
    gated: Dict[str, Dict], project: str, dataset: str, table: str
) -> Optional[Set[str]]:
    """Resolve a gated dataset/table's dataViewer members, or None if unlisted.

    Table-level entries (keyed by version-stripped doctype) override the dataset
    `default`; a listed dataset with no matching table falls back to `default`
    (empty when the dataset grants no broad read access).
    """
    entry = (gated.get(project) or {}).get(dataset)
    if entry is None:
        return None
    tables = entry.get("tables") or {}
    doctype = _VERSION_RE.sub("", table)
    members = tables.get(doctype, tables.get(table, entry.get("default", [])))
    return set(members)


class DatasetAccess:
    """Effective read access for a dataset, from its dataset_metadata.yaml."""

    def __init__(self, readers: Set[str], base_acl: str):
        """Hold a dataset's read-role members and its base ACL archetype."""
        self.readers = readers
        self.base_acl = base_acl

    @property
    def sensitive(self) -> bool:
        """Restricted base ACL, or readers scoped to a non-broad workgroup."""
        if "restricted" in self.base_acl:
            return True
        return bool(self.readers) and not self.readers <= BROAD_READERS


def dataset_access(sql_dir: str, project: str, dataset: str) -> Optional[DatasetAccess]:
    """Resolve a dataset's effective read access, or None if unresolved."""
    metadata_path = Path(sql_dir) / project / dataset / DATASET_METADATA_FILE
    if not metadata_path.exists():
        return None
    try:
        metadata = DatasetMetadata.from_file(metadata_path)
    except Exception:
        return None
    readers: Set[str] = set()
    for entry in metadata.workgroup_access or []:
        if entry.get("role") in READ_ROLES:
            readers.update(entry.get("members", []))
    return DatasetAccess(readers=readers, base_acl=metadata.dataset_base_acl or "")


def table_readers(sql_dir: str, project: str, dataset: str, table: str) -> Set[str]:
    """Read-role members granted at the table level.

    A table's own metadata.yaml `workgroup_access` only widens the dataset's
    grant, so these are unioned into the table's effective readers.
    """
    metadata_path = Path(sql_dir) / project / dataset / table / METADATA_FILE
    if not metadata_path.exists():
        return set()
    try:
        metadata = Metadata.from_file(metadata_path)
    except Exception:
        return set()
    readers: Set[str] = set()
    for entry in metadata.workgroup_access or []:
        if entry.role in READ_ROLES:
            readers.update(entry.members)
    return readers


def effective_access(
    sql_dir: str, project: str, dataset: str, table: str
) -> Optional[DatasetAccess]:
    """Dataset access widened by the table's own workgroup_access."""
    da = dataset_access(sql_dir, project, dataset)
    if da is None:
        return None
    return DatasetAccess(
        readers=da.readers | table_readers(sql_dir, project, dataset, table),
        base_acl=da.base_acl,
    )


def source_access(
    sql_dir: str,
    project: str,
    dataset: str,
    table: str,
    gated: Dict[str, Dict],
) -> Optional[DatasetAccess]:
    """Resolve a source table's effective read access.

    Resolution order:
      1. `gated_datasets` config: authoritative for ingestion (stable/live)
         ACLs, which have no dataset_metadata.yaml in this repo.
      2. in-repo dataset_metadata.yaml (derived/view datasets).
      3. an unlisted stable/live dataset in an ingestion project (a project keyed
         in `gated_datasets`): granted the broad default, so non-sensitive.
      4. None: genuinely unresolved (caller treats as sensitive).
    """
    readers = gated_readers(gated, project, dataset, table)
    if readers is not None:
        return DatasetAccess(readers=readers, base_acl="gated")

    da = effective_access(sql_dir, project, dataset, table)
    if da is not None:
        return da

    if project in gated and dataset.endswith(INGESTION_SUFFIXES):
        return DatasetAccess(readers=set(BROAD_READERS), base_acl="ingestion")

    return None


def _resolve_ref(ref: str, default_project: str) -> Optional[Tuple[str, str, str]]:
    """Return (project, dataset, table) for a table ref, or None if unusable."""
    parts = ref.split(".")
    if len(parts) == 3:
        project, dataset, table = parts
    elif len(parts) == 2:
        project, dataset, table = default_project, parts[0], parts[1]
    else:
        return None
    if dataset == "INFORMATION_SCHEMA":
        return None  # query/job metadata, not a data source
    return project, dataset, table


def load_codeowners(codeowners_file: str) -> List[Tuple[str, List[str]]]:
    """Parse CODEOWNERS into ordered (pattern, owners) entries."""
    entries: List[Tuple[str, List[str]]] = []
    for line in Path(codeowners_file).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pattern, *owners = line.split()
        entries.append((pattern, owners))
    return entries


def path_owners(rel_path: str, entries: List[Tuple[str, List[str]]]) -> List[str]:
    """Owners for a repo-relative path, honoring CODEOWNERS last-match-wins."""
    owners: List[str] = []
    for pattern, pattern_owners in entries:
        spec = pathspec.PathSpec.from_lines("gitwildmatch", [pattern])
        if spec.match_file(rel_path):
            owners = pattern_owners
    return owners


def check_query(
    query_file: str,
    sql_dir: str,
    codeowners: Optional[List[Tuple[str, List[str]]]] = None,
    gated_datasets: Optional[Dict[str, Dict]] = None,
) -> List[Dict]:
    """Flag sensitive-source -> broader-destination flows for one query.sql.

    Returns one finding per sensitive source whose readers don't already cover
    the destination's readers (i.e. the write widens access). When `codeowners`
    is given, each finding records whether the query path is owned (i.e. review
    is required).
    """
    query_file_path = Path(query_file)
    dest_table = query_file_path.parent.name
    dest_dataset = query_file_path.parent.parent.name
    dest_project = query_file_path.parent.parent.parent.name
    dest = effective_access(sql_dir, dest_project, dest_dataset, dest_table)
    if dest is None:
        return []

    # query.sql is a Jinja template; render before parsing
    try:
        sql = render(query_file_path.name, template_folder=query_file_path.parent)
        refs = extract_table_references(sql)
    except Exception:
        return []

    owners = (
        path_owners(str(query_file_path).lstrip("./"), codeowners)
        if codeowners is not None
        else None
    )
    gated = load_gated_datasets() if gated_datasets is None else gated_datasets

    findings: List[Dict] = []
    for ref in refs:
        resolved = _resolve_ref(ref, dest_project)
        if resolved is None:
            continue
        src_project, src_dataset, src_table = resolved
        if src_project in SAFE_PROJECTS:
            continue  # mozfun etc.: public, safe to read
        if (src_project, src_dataset) == (dest_project, dest_dataset):
            continue  # self-reference
        src = source_access(sql_dir, src_project, src_dataset, src_table, gated)
        if src is None:
            # unresolved and not a known-safe project (e.g. other-project refs):
            # can't verify its access, so treat it as sensitive.
            src_readers: Set[str] = set()
            src_base_acl = "unresolved (treated as sensitive)"
        elif src.sensitive:
            src_readers = src.readers
            src_base_acl = src.base_acl
        else:
            continue  # resolved and not sensitive
        # readers the destination grants that the source does not authorize
        widened = dest.readers - src_readers
        if widened:
            findings.append(
                {
                    "query_path": str(query_file_path),
                    "query": f"{dest_project}.{dest_dataset}",
                    "source": f"{src_project}.{src_dataset}",
                    "source_readers": sorted(src_readers),
                    "source_base_acl": src_base_acl,
                    "destination_readers": sorted(dest.readers),
                    "extra_readers": sorted(widened),
                    "owners": owners,
                    # gated == a reviewer is required for this path
                    "gated": bool(owners) if owners is not None else None,
                }
            )
    return findings


def check_paths(
    paths: List[str],
    sql_dir: str,
    codeowners_file: Optional[str] = None,
) -> List[Dict]:
    """Run check_query over query.sql files under the given paths."""
    codeowners = load_codeowners(codeowners_file) if codeowners_file else None
    findings: List[Dict] = []
    for p in paths:
        path = Path(p)
        if path.name == "query.sql":
            query_files = [path]
        elif path.is_dir():
            query_files = sorted(path.rglob("query.sql"))
        else:
            continue  # non-SQL artifact (e.g. query.py)
        for query_file_path in query_files:
            findings.extend(check_query(str(query_file_path), sql_dir, codeowners))
    return findings


def format_findings(findings: List[Dict]) -> str:
    """Human-readable advisory, one block per source -> destination pair."""
    pairs: Dict[Tuple[str, str], Dict] = {}
    for f in findings:
        pairs.setdefault((f["source"], f["query"]), f)
    lines = []
    for (source, dest), f in sorted(pairs.items()):
        gate = (
            "needs Data Platform (@mozilla/dataplatform-wg) review"
            if not f["gated"]
            else f"already reviewed by {', '.join(f['owners'])}"
        )
        lines.append(
            f"- {source} ({f['source_base_acl'] or 'gated'}) -> {dest}\n"
            f"    grants read to {', '.join(f['extra_readers'])} "
            f"not authorized on the source\n"
            f"    {gate}"
        )
    return "\n".join(lines)
