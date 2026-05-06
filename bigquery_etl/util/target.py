"""Utilities for managing target environments for query deployment."""

import logging
import os
import re
import shutil
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import List, NamedTuple, Optional, Set, Tuple

import attr
import cattrs
import click
import git
import google.auth.transport.requests
import jinja2
import requests
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Template

from bigquery_etl.routine.parse_routine import (
    PERSISTENT_UDF_RE,
    ROUTINE_FILES,
    read_routine_dir,
    routine_usage_pattern,
    routine_usages_in_text,
)

from ..config import ConfigLoader
from ..dependency import extract_table_references
from ..deploy import deploy_table
from ..dryrun import get_id_token
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..schema import SCHEMA_FILE, Schema
from ..view import View
from . import extract_from_query_path
from .common import get_bqetl_project_root
from .common import render as render_template

VIEW_FILE = "view.sql"
QUERY_FILE = "query.sql"
QUERY_SCRIPT = "query.py"
MATERIALIZED_VIEW = "materialized_view.sql"

ROOT = Path(__file__).parent.parent.parent

MANIFEST_FILENAME = ".bqetl_target_info.yaml"
DEFAULT_TARGETS_FILENAME = "bqetl_targets.yaml"


class DeployedTableInfo(NamedTuple):
    """Info about a table deployed to a target environment."""

    target_project: str
    target_dataset: str
    target_table: str
    source_project: Optional[str] = None
    source_dataset: Optional[str] = None
    source_table: Optional[str] = None


class _KeepUndefined(jinja2.Undefined):
    """Jinja2 Undefined that preserves {{ var.attr }} expressions in output instead of raising."""

    def __str__(self) -> str:
        return "{{ " + (self._undefined_name or "") + " }}"

    def __getattr__(self, name: str) -> "_KeepUndefined":
        if name.startswith("__"):
            raise AttributeError(name)
        return _KeepUndefined(name=f"{self._undefined_name}.{name}")

    def __getitem__(self, key: object) -> "_KeepUndefined":  # type: ignore[override]
        return _KeepUndefined(name=f"{self._undefined_name}[{key!r}]")


def sanitize_bq_id(value: str) -> str:
    """Sanitize a BigQuery identifier (project, dataset, or table) to conform to requirements.

    Identifiers must be alphanumeric (plus underscores) and at most 1024 characters.
    Replaces hyphens and other invalid characters with underscores.
    """
    # replace hyphens and other non-alphanumeric characters (except underscores) with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    return sanitized[:1024]


@attr.s()
class Target:
    """Target configuration for deployment."""

    name: str = attr.ib()
    project_id: str = attr.ib()
    dataset_prefix: Optional[str] = attr.ib(default=None)
    dataset: Optional[str] = attr.ib(default=None)
    artifact_prefix: Optional[str] = attr.ib(default=None)
    # When True, datasets created by this target's deploys grant READER to
    # `dry_run.function_accounts`. Use for shared/CI environments (e.g. stage)
    # where the cloud-function dry-run needs to read staged data.
    grant_dryrun_access: bool = attr.ib(default=False)
    # Default table expiration (in hours) on datasets this target creates.
    # When set, the dataset is also labeled `expires_on=<unix-millis>` so a
    # sweeper can GC datasets.
    expire_after_hours: Optional[int] = attr.ib(default=None)
    # When True, --target deploys copy and rename SQL tests under tests/sql/
    # to match target paths so pytest can run against staged artifacts.
    rewrite_tests: bool = attr.ib(default=False)

    # raw (unrendered) templates — preserved so that pattern-matching code
    # (e.g. target clean) can parameterize git.branch / git.commit independently.
    raw_dataset_prefix: Optional[str] = attr.ib(default=None)
    raw_dataset: Optional[str] = attr.ib(default=None)
    raw_artifact_prefix: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        """Check attributes."""
        if self.dataset is not None and self.dataset_prefix is not None:
            raise ValueError(
                "Cannot specify both 'dataset' and 'dataset_prefix' in a target"
            )


def _template_to_pattern(
    template_str: str,
    branch: Optional[str] = None,
    run_id: Optional[str] = None,
    anchor_end: bool = False,
    capture_commit: bool = False,
) -> str:
    """Render a Jinja2 template into a regex pattern for matching BQ identifiers.

    Known values (branch, username, run_id) are rendered literally; the commit
    slot must start with 7+ hex chars (short SHA) and may have trailing chars
    from legacy templates; other variable slots become [a-zA-Z0-9_]+. Anchoring
    the commit slot to a hex SHA prefix prevents over-matching when the
    literal branch is a substring of another branch's sanitized name.

    If capture_commit is True, the first commit slot is wrapped in a
    (non-greedy) capture group so the commit can be extracted from a name
    that matches the pattern.

    Returns a ^-anchored regex string, optionally $-anchored.
    """
    _WILDCARD = "XBQETLWCX"
    _COMMIT_WILDCARD = "XBQETLCOMMITX"
    rendered = Template(template_str).render(
        git={"branch": branch or _WILDCARD, "commit": _COMMIT_WILDCARD},
        account=_get_account_context(),
        artifact={"project_id": _WILDCARD, "dataset_id": _WILDCARD},
        run_id=run_id if run_id else _WILDCARD,
    )

    escaped = re.escape(sanitize_bq_id(rendered))
    if capture_commit:
        escaped = escaped.replace(_COMMIT_WILDCARD, "([a-f0-9]{7,}[a-zA-Z0-9_]*?)", 1)
    pattern = escaped.replace(_COMMIT_WILDCARD, "[a-f0-9]{7,}[a-zA-Z0-9_]*").replace(
        _WILDCARD, "[a-zA-Z0-9_]+"
    )

    return f"^{pattern}$" if anchor_end else f"^{pattern}"


def render_dataset_pattern(
    target: Target,
    branch: Optional[str] = None,
    run_id: Optional[str] = None,
) -> str:
    """Render a target's dataset template into a regex pattern for matching dataset names."""
    template_str = target.raw_dataset or target.raw_dataset_prefix
    if not template_str:
        raise click.ClickException(
            f"Target '{target.name}' has no dataset or dataset_prefix template. "
            "Cannot determine which datasets belong to this target."
        )
    return _template_to_pattern(
        template_str,
        branch=branch,
        run_id=run_id,
        anchor_end=bool(target.raw_dataset),
    )


def render_artifact_prefix_pattern(
    target: Target,
    branch: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Optional[str]:
    """Render a target's artifact_prefix template into a regex pattern for matching table names."""
    if not target.raw_artifact_prefix:
        return None
    return _template_to_pattern(
        target.raw_artifact_prefix, branch=branch, run_id=run_id
    )


def extract_commit_from_dataset_name(
    target: Target, dataset_id: str, branch: str
) -> Optional[str]:
    """Extract the git.commit value from a dataset name using the target's template.

    Returns None if the template has no git.commit slot or the name does not
    match the expected pattern.
    """
    template_str = target.raw_dataset or target.raw_dataset_prefix
    if not template_str or "git.commit" not in template_str:
        return None
    pattern = _template_to_pattern(
        template_str,
        branch=branch,
        anchor_end=bool(target.raw_dataset),
        capture_commit=True,
    )
    m = re.match(pattern, dataset_id)
    return m.group(1) if m else None


def render_artifact_template(
    template: Optional[str], project_id: str, dataset_id: str = ""
) -> Optional[str]:
    """Render {{ artifact.* }} variables in a target config template string."""
    if not template or "{{" not in str(template):
        return template
    return Template(str(template), undefined=jinja2.StrictUndefined).render(
        artifact={
            "project_id": sanitize_bq_id(project_id or ""),
            "dataset_id": sanitize_bq_id(dataset_id or ""),
        }
    )


def _get_targets_file() -> Path:
    """Return the path to the targets config file (always in bigquery-etl)."""
    targets_file_name = ConfigLoader.get(
        "default", "targets", fallback=DEFAULT_TARGETS_FILENAME
    )
    return ConfigLoader.project_dir / targets_file_name


def _git_env_branch() -> Optional[str]:
    """Return a branch name from GitHub Actions env vars, or None."""
    return (
        os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME") or None
    )


def _git_env_commit() -> Optional[str]:
    """Return a commit SHA from GitHub Actions env vars, or None."""
    return os.environ.get("GITHUB_SHA") or None


@cache
def _get_git_context() -> dict:
    """Return git template variables, cached after first call.

    CI checkouts often use a detached HEAD (`actions/checkout` with a SHA ref),
    so `repo.active_branch` raises. Fall back to GitHub Actions env vars before
    giving up so target dataset/artifact templates resolve correctly in CI.
    """
    branch: Optional[str] = None
    commit: Optional[str] = None
    try:
        project_root = get_bqetl_project_root() or ROOT
        repo = git.Repo(project_root)
        try:
            branch = repo.active_branch.name
        except TypeError:
            branch = _git_env_branch()
        try:
            commit = repo.head.commit.hexsha
        except Exception:
            commit = _git_env_commit()
    except Exception:
        branch = _git_env_branch()
        commit = _git_env_commit()

    if not branch or not commit:
        logging.warning(
            "Could not determine git branch/commit. Using 'unknown' for missing values."
        )
    return {"branch": branch or "unknown", "commit": commit or "unknown"}


@cache
def _get_run_id() -> str:
    """Return a per-invocation run id from env, or empty string.

    Used by target dataset/artifact templates as `{{ run_id }}` to disambiguate
    parallel deploys for the same git.branch/git.commit (e.g. concurrent CI
    runs). `BQETL_RUN_ID` takes precedence; `GITHUB_RUN_ID` is the GitHub
    Actions fallback so CI doesn't have to forward it explicitly.
    """
    return os.environ.get("BQETL_RUN_ID") or os.environ.get("GITHUB_RUN_ID") or ""


@cache
def _get_gcloud_account() -> str:
    """Return the active GCP account email from credentials, cached after first call.

    Returns an empty string if the account cannot be determined.
    """
    try:
        client = bigquery.Client()
        credentials = client._credentials
        if not credentials.valid:
            credentials.refresh(google.auth.transport.requests.Request())
        return (
            requests.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"access_token": credentials.token},
            )
            .json()
            .get("email", "")
        )
    except Exception as e:
        logging.warning(f"Could not determine GCP account from credentials: {e}")
        return ""


@cache
def _get_account_context() -> dict:
    """Return account template variables, cached after first call."""
    account = _get_gcloud_account()
    if account:
        username = account.split("@")[0]
    else:
        logging.warning(
            "Could not determine GCP account. Using 'unknown' for account.username"
        )
        username = "unknown"
    return {"username": username}


def get_target(target: str) -> Target:
    """Load and return a Target from the targets config file by name."""
    targets_file = _get_targets_file()

    if not targets_file.exists():
        raise Exception(f"Targets file not found: {targets_file}")

    raw_content = targets_file.read_text()

    # Parse raw YAML to capture unrendered templates
    raw_targets = yaml.safe_load(raw_content)
    if not isinstance(raw_targets, dict) or target not in raw_targets:
        raise Exception(f"Couldn't find target `{target}` in {targets_file}")

    raw_cfg = raw_targets[target] or {}

    # Render git/account variables for the resolved Target
    template = Template(raw_content, undefined=_KeepUndefined)
    rendered_content = template.render(
        git=_get_git_context(),
        account=_get_account_context(),
        run_id=_get_run_id(),
    )

    targets = yaml.safe_load(rendered_content)

    result = cattrs.structure({**targets[target], "name": target}, Target)
    result.raw_dataset = raw_cfg.get("dataset")
    result.raw_dataset_prefix = raw_cfg.get("dataset_prefix")
    result.raw_artifact_prefix = raw_cfg.get("artifact_prefix")
    return result


def get_default_target_name() -> Optional[str]:
    """Return the default target name, checking sources in priority order.

    1. BQETL_TARGET environment variable
    2. default_target key in bqetl_targets.yaml
    """
    env_target = os.environ.get("BQETL_TARGET")
    if env_target:
        return env_target

    targets_file = _get_targets_file()

    if not targets_file.exists():
        return None

    targets = yaml.safe_load(targets_file.read_text())
    if isinstance(targets, dict):
        return targets.get("default_target")

    return None


def _artifact_exists(
    client: bigquery.Client,
    target_project: str,
    dataset: str,
    name: str,
    is_routine: bool,
) -> bool:
    """Return True if the artifact (table or routine) exists in BigQuery."""
    ref = f"{target_project}.{dataset}.{name}"
    try:
        if is_routine:
            client.get_routine(ref)
        else:
            client.get_table(ref)
        return True
    except NotFound:
        return False


def _get_deployed_artifacts_in_target(
    sql_dir: str,
    target_project: str,
    file_patterns: tuple,
    is_routine: bool = False,
) -> Set[DeployedTableInfo]:
    """Find artifacts deployed in the target directory.

    Scans for files matching file_patterns under <sql_dir>/<target_project>/,
    checks that the artifact exists in BigQuery, and reads the source manifest.
    """
    deployed: Set[DeployedTableInfo] = set()
    target_project_dir = Path(sql_dir) / target_project

    if not target_project_dir.exists():
        return deployed

    client = bigquery.Client(project=target_project)
    seen_dirs: Set[Path] = set()
    for pattern in file_patterns:
        for artifact_file in target_project_dir.rglob(pattern):
            if artifact_file.parent in seen_dirs:
                continue
            seen_dirs.add(artifact_file.parent)

            if artifact_file.parent.parent.parent != target_project_dir:
                continue

            name = artifact_file.parent.name
            dataset = artifact_file.parent.parent.name

            if not _artifact_exists(client, target_project, dataset, name, is_routine):
                continue

            manifest_file = artifact_file.parent / MANIFEST_FILENAME
            if not manifest_file.exists():
                if is_routine:
                    continue
                deployed.add(
                    DeployedTableInfo(
                        target_project=target_project,
                        target_dataset=dataset,
                        target_table=name,
                    )
                )
                continue

            try:
                manifest = yaml.safe_load(manifest_file.read_text())
            except Exception:
                continue

            source_project = manifest.get("source_project")
            source_dataset = manifest.get("source_dataset")
            source_table = manifest.get("source_table")

            if is_routine and not all([source_project, source_dataset, source_table]):
                continue

            deployed.add(
                DeployedTableInfo(
                    target_project=target_project,
                    target_dataset=dataset,
                    target_table=name,
                    source_project=source_project,
                    source_dataset=source_dataset,
                    source_table=source_table,
                )
            )

    return deployed


def get_deployed_tables_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all tables deployed in the target directory."""
    return _get_deployed_artifacts_in_target(
        sql_dir, target_project, ("query.sql", "script.sql", "part1.sql")
    )


def get_deployed_routines_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all routines deployed in the target directory."""
    return _get_deployed_artifacts_in_target(
        sql_dir, target_project, ROUTINE_FILES, is_routine=True
    )


def _normalize_table_ref(
    dependency: str, default_project: str
) -> Optional[Tuple[str, str, str]]:
    """Split a table reference into (project, dataset, name).

    Promotes 2-part INFORMATION_SCHEMA refs to 3-part with default_project,
    and folds the trailing INFORMATION_SCHEMA pseudo-table-path into one name
    so we don't try to deploy it.
    """
    components = dependency.split(".")
    if components[1:2] == ["INFORMATION_SCHEMA"]:
        components.insert(0, default_project)
    if components[2:3] == ["INFORMATION_SCHEMA"]:
        components = components[:2] + [".".join(components[2:])]
    if len(components) != 3:
        return None
    project, dataset, name = components
    return project, dataset, name


def _existing_artifact_file(source_dir: Path) -> Optional[Path]:
    """Return the recognized artifact file under source_dir, or None."""
    for fn in (VIEW_FILE, QUERY_FILE, QUERY_SCRIPT, MATERIALIZED_VIEW):
        if (source_dir / fn).is_file():
            return source_dir / fn
    return None


def _fetch_stub_schema(
    project: str,
    dataset: str,
    name: str,
    out_path: Path,
    id_token: str,
) -> None:
    """Write schema.yaml for an unmanaged dep table at out_path.

    Tries `client.get_table` first (fast, works on partition-required tables),
    falls back to `Schema.for_table`'s dry-run for cases where the source
    table doesn't exist yet. Logs both errors when both fail.
    """
    get_table_err: Optional[Exception] = None
    try:
        bq_table = bigquery.Client(project=project).get_table(
            f"{project}.{dataset}.{name}"
        )
        Schema.from_bigquery_schema(bq_table.schema).to_yaml_file(out_path)
        return
    except Exception as e:
        get_table_err = e

    partitioned_by = (
        "submission_timestamp"
        if any(dataset.endswith(s) for s in ("_live", "_stable"))
        else None
    )
    try:
        Schema.for_table(
            project=project,
            dataset=dataset,
            table=name,
            id_token=id_token,
            partitioned_by=partitioned_by,
        ).to_yaml_file(out_path)
    except Exception as for_table_err:
        print(
            f"Warning: Could not fetch schema for {project}.{dataset}.{name}: "
            f"get_table: {get_table_err}; dry-run: {for_table_err}"
        )


def _create_target_stub(
    project: str,
    dataset: str,
    name: str,
    sql_dir: str,
    target: Target,
    id_token: str,
) -> Path:
    """Write a stub for an unmanaged dependency table.

    Drops a placeholder ``query.py`` and a best-effort ``schema.yaml`` at
    the target path so the regular deploy flow can pick it up.
    """
    is_wildcard = "*" in name
    stub_name = name.replace("*", "wildcard") if is_wildcard else name
    tgt_project, tgt_dataset, tgt_table = _target_ref_for_source(
        target, target.project_id, project, dataset, stub_name
    )
    stub_path = Path(sql_dir) / tgt_project / tgt_dataset / tgt_table
    stub_path.mkdir(parents=True, exist_ok=True)

    # Wildcards represent multiple tables; no single schema to fetch.
    if not is_wildcard:
        _fetch_stub_schema(project, dataset, name, stub_path / SCHEMA_FILE, id_token)

    (stub_path / QUERY_SCRIPT).write_text("# Table stub generated by --target deploy")
    return stub_path / QUERY_SCRIPT


def _table_refs_from(dep_file: Path) -> List[str]:
    """Return the table references from a view, query, or materialized view.

    Query files with a checked-in schema.yaml return [] — we deploy the
    schema structure rather than running the query, so we don't need to
    walk into the query's own deps.
    """
    if dep_file.name == VIEW_FILE:
        return View.from_file(dep_file, id_token=get_id_token()).table_references
    if dep_file.name in (QUERY_FILE, QUERY_SCRIPT, MATERIALIZED_VIEW):
        if (dep_file.parent / SCHEMA_FILE).exists():
            return []
        try:
            sql_content = render_template(
                dep_file.name, template_folder=dep_file.parent
            )
            return extract_table_references(sql_content)
        except Exception as e:
            print(f"Warning: Could not extract references from {dep_file}: {e}")
    return []


def _udf_refs_from(dep_file: Path) -> List[str]:
    """Return the UDF references from a view, query, or materialized view."""
    if dep_file.name == VIEW_FILE:
        return View.from_file(dep_file, id_token=get_id_token()).udf_references
    if dep_file.name in (QUERY_FILE, MATERIALIZED_VIEW):
        try:
            sql_content = render_template(
                dep_file.name, template_folder=dep_file.parent, format=False
            )
            return routine_usages_in_text(
                sql_content, dep_file.parent.parent.parent.name
            )
        except Exception as e:
            print(f"Warning: Could not extract UDF refs from {dep_file}: {e}")
    return []


def _udf_dep_paths(udf_names: List[str]) -> Set[Path]:
    """Resolve UDF names + their transitive deps to source filesystem paths."""
    if not udf_names:
        return set()
    # local import to avoid circular deps
    from bigquery_etl.routine.parse_routine import accumulate_dependencies

    raw_routines = read_routine_dir()
    paths: Set[Path] = set()
    for udf in udf_names:
        if udf not in raw_routines:
            continue
        for transitive in accumulate_dependencies([], raw_routines, udf):
            if transitive in raw_routines:
                paths.add(Path(raw_routines[transitive].filepath))
    return paths


def collect_target_dependencies(
    artifact_files: Set[Path],
    sql_dir: str,
    target: Target,
) -> Set[Path]:
    """Walk dependencies of `artifact_files` for an --isolated target deploy.

    Behavior parallels `bigquery_etl.cli.stage.collect_artifact_dependencies`,
    but stubs for unmanaged tables (live/stable, syndicated, etc. — anything
    referenced by deployed artifacts but not present under sql/) are written
    *directly* into the target tree at
    `sql/<target_project>/<target_dataset>/<target_artifact>/`, computed via
    the target's templates. Source-managed deps are returned as their
    sql/<source>/... paths and the regular deploy flow rewrites them into the
    target via prepare_target_files.

    Returns the set of dep paths to deploy (mix of source paths and target
    paths). Callers detect already-target paths to skip prepare_target_files.
    """
    artifact_dependencies: Set[Path] = set()
    dependency_files = [
        f
        for f in artifact_files
        if f.name in (VIEW_FILE, QUERY_FILE, MATERIALIZED_VIEW)
    ]
    id_token = get_id_token()

    # Visit each file and each (project, dataset, name) ref at most once. The
    # same dep is commonly referenced from many artifacts, and without dedup
    # `_create_target_stub` re-runs `_fetch_stub_schema` (dry-run) on every
    # occurrence, blowing up CI logs and runtime.
    walked_files: Set[Path] = set()
    seen_refs: Set[Tuple[str, str, str]] = set()

    for dep_file in dependency_files:
        if dep_file in walked_files:
            continue
        walked_files.add(dep_file)

        if dep_file not in artifact_files:
            artifact_dependencies.add(dep_file)

        # Walk table refs — managed deps recurse via dependency_files; unmanaged
        # deps get a stub written directly at their target path.
        artifact_project = dep_file.parent.parent.parent.name
        for ref in _table_refs_from(dep_file):
            normalized = _normalize_table_ref(ref, artifact_project)
            if normalized is None:
                raise ValueError(
                    f"Invalid table reference {ref} in {dep_file}. "
                    "Expected format: project.dataset.table."
                )
            project, dataset, name = normalized
            if dataset == "INFORMATION_SCHEMA" or "INFORMATION_SCHEMA" in name:
                continue
            if (project, dataset, name) in seen_refs:
                continue
            seen_refs.add((project, dataset, name))

            existing = _existing_artifact_file(Path(sql_dir) / project / dataset / name)
            if existing is not None:
                if existing not in artifact_files:
                    dependency_files.append(existing)
                continue

            artifact_dependencies.add(
                _create_target_stub(project, dataset, name, sql_dir, target, id_token)
            )

        # UDF refs — paths come from sql/<source>/... directly (UDFs go through
        # the regular routine publish step; we just need their paths).
        artifact_dependencies.update(_udf_dep_paths(_udf_refs_from(dep_file)))

    return artifact_dependencies


def _target_ref_for_source(
    target: "Target",
    target_project: str,
    src_project: str,
    src_dataset: str,
    src_table: str,
) -> Tuple[str, str, str]:
    """Compute the target-environment 3-part location for a source reference.

    Single source of truth for "given a source project.dataset.table, where
    does it land in target?". Used by:
    - prepare_target_directory (where to copy artifact files)
    - collect_target_dependencies (where to write stubs for unmanaged tables)
    - rewrite_for_isolated (where to point rewritten 3-part refs)
    """
    rendered_artifact_prefix = sanitize_bq_id(
        render_artifact_template(target.artifact_prefix, src_project, src_dataset) or ""
    )
    if target.dataset:
        rendered = render_artifact_template(target.dataset, src_project, src_dataset)
        target_ds = sanitize_bq_id(rendered) if rendered else src_dataset
    elif target.dataset_prefix:
        rendered = render_artifact_template(
            target.dataset_prefix, src_project, src_dataset
        )
        prefix = sanitize_bq_id(rendered) if rendered else ""
        target_ds = sanitize_bq_id(f"{prefix}{src_dataset}")
    else:
        target_ds = src_dataset
    target_table = (
        sanitize_bq_id(f"{rendered_artifact_prefix}{src_table}")
        if rendered_artifact_prefix
        else src_table
    )
    return target_project, target_ds, target_table


def _read_source_project_from_manifest(query_file: Path) -> Optional[str]:
    """Recover the artifact's original source project from its target manifest."""
    manifest_path = query_file.parent / MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    try:
        return (yaml.safe_load(manifest_path.read_text()) or {}).get("source_project")
    except Exception:
        return None


def read_source_identity_from_manifest(
    query_file: Path,
) -> Optional[Tuple[str, str, str]]:
    """Recover (source_project, source_dataset, source_table) from the manifest.

    Returns None if the manifest is missing, unreadable, or doesn't contain all
    three source fields. Used by deploy-time schema resolution to point the
    `client.get_table` lookup at the original prod table.
    """
    manifest_path = query_file.parent / MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    try:
        manifest = yaml.safe_load(manifest_path.read_text()) or {}
    except Exception:
        return None
    src_project = manifest.get("source_project")
    src_dataset = manifest.get("source_dataset")
    src_table = manifest.get("source_table")
    if not (src_project and src_dataset and src_table):
        return None
    return src_project, src_dataset, src_table


def _substitute_3part_ref(
    sql: str,
    src: Tuple[str, str, str],
    tgt: Tuple[str, str, str],
) -> str:
    """Replace every occurrence of source 3-part ref with target 3-part ref."""
    src_project, src_dataset, src_table = src
    pattern = re.compile(
        rf"`?{re.escape(src_project)}`?"
        rf"\.`?{re.escape(src_dataset)}`?"
        rf"\.`?{re.escape(src_table)}\b`?"
    )
    return pattern.sub(f"`{tgt[0]}`.`{tgt[1]}`.`{tgt[2]}`", sql)


def rewrite_for_isolated(
    query_file: Path,
    sql_dir: str,
    target_project: str,
    target: "Target",
) -> None:
    """Rewrite ALL references in `query_file` to point at the target.

    Used by --isolated deploys: every project.dataset.table in the SQL is
    re-rendered through the target's templates, plus 2-part UDF calls
    (e.g. `json.extract_int_map`) that 3-part extraction doesn't see.
    """
    sql = render_template(
        query_file.name, template_folder=str(query_file.parent), format=False
    )

    # sqlglot extraction excludes struct field paths like `metadata.header.date`
    # and CREATE-clause self-refs, so we don't need a known-projects heuristic
    # or a target_project skip guard.
    for ref in extract_table_references(sql):
        parts = ref.split(".")
        if len(parts) != 3 or parts[0] == target_project:
            continue
        src_project, src_dataset, src_table = parts
        sql = _substitute_3part_ref(
            sql,
            (src_project, src_dataset, src_table),
            _target_ref_for_source(
                target, target_project, src_project, src_dataset, src_table
            ),
        )

    # 2-part UDF refs (e.g. `json.extract_int_map` inside `mozfun.json.extract`)
    # aren't 3-part extractable. Walk known routines under the file's source
    # project and rewrite their 2-part usages.
    file_source_project = _read_source_project_from_manifest(query_file)
    if file_source_project:
        for routine_name, routine in read_routine_dir().items():
            if routine.project != file_source_project:
                continue
            src_dataset, src_name = routine_name.split(".")
            tgt = _target_ref_for_source(
                target, target_project, routine.project, src_dataset, src_name
            )
            two_part = re.compile(
                rf"(?<![\w\.`])`?{re.escape(src_dataset)}`?"
                rf"\.`?{re.escape(src_name)}`?(?=\()"
            )
            sql = two_part.sub(f"`{tgt[0]}`.`{tgt[1]}`.`{tgt[2]}`", sql)

    query_file.write_text(sql)


def rewrite_for_defer(
    query_file: Path,
    sql_dir: str,
    target_project: str,
    target: "Target",
) -> None:
    """Smart-rewrite refs that are already deployed in target; leave others alone.

    Used by --defer-to-target: prod refs that haven't been deployed to target
    pass through unchanged so the deploy still picks up production data.
    """
    sql = render_template(
        query_file.name, template_folder=str(query_file.parent), format=False
    )

    def _validated_source(
        info: DeployedTableInfo,
    ) -> Optional[Tuple[str, str, str]]:
        """Return the source 3-part ref if `info` is current and complete.

        Returns None when the deployed artifact is missing source fields, or
        when its target dataset doesn't match what the current target config
        would produce — guards against stale deployments from a different
        branch/config.
        """
        if not (info.source_project and info.source_dataset and info.source_table):
            return None
        _, expected_ds, _ = _target_ref_for_source(
            target,
            target_project,
            info.source_project,
            info.source_dataset,
            info.source_table,
        )
        if info.target_dataset != expected_ds:
            return None
        return info.source_project, info.source_dataset, info.source_table

    for info in get_deployed_tables_in_target(sql_dir, target_project):
        src = _validated_source(info)
        if src is None:
            continue
        sql = _substitute_3part_ref(
            sql,
            src,
            (target_project, info.target_dataset, info.target_table),
        )

    # Routine refs can be 2-part (dataset.name) or 3-part (project.dataset.name);
    # routine_usage_pattern handles both, gated on a `(` call site.
    for info in get_deployed_routines_in_target(sql_dir, target_project):
        src = _validated_source(info)
        if src is None:
            continue
        src_project, src_dataset, src_table = src
        udf_pattern = routine_usage_pattern(f"{src_dataset}.{src_table}", src_project)
        sql = udf_pattern.sub(
            f"`{target_project}`.`{info.target_dataset}`.`{info.target_table}`",
            sql,
        )

    query_file.write_text(sql)


def rewrite_query_references(
    query_file: Path,
    sql_dir: str,
    target_project: str,
    target: "Target",
    rewrite_all: bool = False,
) -> None:
    """Dispatch to the appropriate rewrite based on deploy mode.

    Kept as a thin shim for callers that don't want to know about the
    --isolated vs --defer-to-target distinction.
    """
    if rewrite_all:
        rewrite_for_isolated(query_file, sql_dir, target_project, target)
    else:
        rewrite_for_defer(query_file, sql_dir, target_project, target)


def prepare_target_directory(
    query_file: Path,
    sql_dir: str,
    target: "Target",
    defer_to_target: bool,
    isolated: bool,
    copied_target_dirs: Optional[Set[Path]] = None,
) -> Path:
    """Prepare target directory for query execution with --target."""
    source_project, source_dataset, source_table = extract_from_query_path(query_file)
    effective_project, effective_dataset, effective_table = _target_ref_for_source(
        target,
        target.project_id or source_project,
        source_project,
        source_dataset,
        source_table,
    )

    target_dir = Path(sql_dir) / effective_project / effective_dataset / effective_table
    target_query_file = target_dir / query_file.name

    if target_dir == query_file.parent:
        return query_file

    target_dir.mkdir(parents=True, exist_ok=True)

    # Copy source files and write the manifest once per target directory per
    # prepare_target_files() call, so reruns always get fresh source files while
    # multiple files from the same table don't overwrite each other's rewrites.
    if copied_target_dirs is None or target_dir not in copied_target_dirs:
        source_dir = query_file.parent
        for item in source_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, target_dir / item.name)

        # external_data sources (e.g. Google Sheets) can't be recreated in the
        # target project. Clear it so the table deploys from schema.yaml only,
        # matching legacy `stage deploy` behavior.
        target_metadata_file = target_dir / METADATA_FILE
        if target_metadata_file.exists():
            try:
                target_metadata = Metadata.from_file(target_metadata_file)
                if target_metadata.external_data:
                    target_metadata.external_data = None
                    target_metadata.write(target_metadata_file)
            except Exception:
                pass

        # Preserve non-source keys (e.g. shared_with from `target share`) so
        # _reapply_shared_access can re-apply them after re-deploy.
        manifest_path = target_dir / MANIFEST_FILENAME
        existing: dict = {}
        if manifest_path.exists():
            existing = yaml.safe_load(manifest_path.read_text()) or {}

        manifest = {
            **existing,
            "source_project": source_project,
            "source_dataset": source_dataset,
            "source_table": source_table,
        }
        manifest_path.write_text(yaml.dump(manifest))

        if copied_target_dirs is not None:
            copied_target_dirs.add(target_dir)

    # for view and materialized view files, always rewrite the CREATE OR REPLACE VIEW
    # self-reference to match the target directory structure
    if target_query_file.name in ("view.sql", "materialized_view.sql"):
        sql = target_query_file.read_text()
        sql = re.sub(
            r"(CREATE\s+OR\s+REPLACE\s+VIEW\s+)`?[^`.\s]+"
            r"`?\.`?[^`.\s]+`?\.`?[^`.\s]+`?",
            rf"\1`{effective_project}.{effective_dataset}.{effective_table}`",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        target_query_file.write_text(sql)

    # for routine files, rewrite the CREATE FUNCTION/PROCEDURE self-reference
    if target_query_file.name in ("udf.sql", "stored_procedure.sql"):
        sql = target_query_file.read_text()
        sql = PERSISTENT_UDF_RE.sub(
            rf"\g<prefix>`{effective_dataset}`.`{effective_table}`",
            sql,
            count=1,
        )
        target_query_file.write_text(sql)

    if defer_to_target or isolated:
        rewrite_query_references(
            target_query_file,
            sql_dir,
            effective_project,
            target,
            rewrite_all=isolated,
        )

    return target_query_file


def ensure_dataset_exists(
    client: bigquery.Client,
    dataset_ref: str,
    expiration_hours: Optional[int] = None,
    grant_dryrun_access: bool = False,
) -> bool:
    """Create a dataset if it doesn't exist, with user-only access permissions.

    `expiration_hours`: when set, applies default table-expiration and an
    `expires_on` label so a sweeper can GC datasets — used by ephemeral CI
    deploys.

    `grant_dryrun_access`: when True, dry-run service accounts (from
    `dry_run.function_accounts` config) get READER access. Off by default so
    personal dev datasets stay private; opt in for stage / shared CI targets
    by setting `grant_dryrun_access: true` on the target in `bqetl_targets.yaml`.
    """
    try:
        client.get_dataset(dataset_ref)
        return True
    except NotFound:
        pass

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    access_entries = []
    try:
        user_email = _get_gcloud_account()
        # Explicitly grant ownership to the user. When running as yourself this
        # is redundant (BigQuery auto-grants dataOwner to the creator), but with
        # service account impersonation the SA would become the owner instead.
        if user_email and "@" in user_email:
            access_entries.append(
                bigquery.AccessEntry(
                    role="OWNER",
                    entity_type="userByEmail",
                    entity_id=user_email,
                )
            )
        else:
            click.echo("⚠️  Could not determine user email, using default permissions")
    except Exception as e:
        click.echo(f"⚠️  Could not set dataset permissions: {e}")

    # Grant READER to dry-run cloud function accounts so schema dry-runs can
    # read staged datasets. Opt-in per-target — leave off for personal dev so
    # the cloud-function service account doesn't get access to your data.
    if grant_dryrun_access:
        for dry_run_account in ConfigLoader.get(
            "dry_run", "function_accounts", fallback=[]
        ):
            access_entries.append(
                bigquery.AccessEntry(
                    role="READER",
                    entity_type="userByEmail",
                    entity_id=dry_run_account,
                )
            )

    if access_entries:
        dataset.access_entries = access_entries

    if expiration_hours is not None:
        # Both per-table default expiration and a label so a sweeper can find
        # datasets to GC — same shape legacy stage uses.
        dataset.default_table_expiration_ms = expiration_hours * 60 * 60 * 1000
        expires_on = int(
            (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000
            + expiration_hours * 60 * 60 * 1000
        )
        dataset.labels = {**(dataset.labels or {}), "expires_on": str(expires_on)}

    try:
        client.create_dataset(dataset, exists_ok=True)
        click.echo(f"✅ Created dataset: {dataset_ref}")
        return True
    except Exception as e:
        click.echo(f"⚠️  Failed to create dataset: {e}")
        return False


IAM_ROLE_MAP = {
    "READER": "roles/bigquery.dataViewer",
    "WRITER": "roles/bigquery.dataEditor",
    "OWNER": "roles/bigquery.dataOwner",
}


def _reapply_shared_access(client: bigquery.Client, table_ref: str, query_file: Path):
    """Re-apply table-level sharing from the manifest after deploy."""
    manifest_path = query_file.parent / MANIFEST_FILENAME
    if not manifest_path.exists():
        return

    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    shared_with = manifest.get("shared_with", [])
    if not shared_with:
        return

    try:
        table = client.get_table(table_ref)
        policy = client.get_iam_policy(table)

        for entry in shared_with:
            iam_role = IAM_ROLE_MAP.get(entry["role"])
            if not iam_role:
                continue
            member = f"user:{entry['email']}"

            already = any(
                b["role"] == iam_role and member in b.get("members", set())
                for b in policy.bindings
            )
            if not already:
                policy.bindings.append({"role": iam_role, "members": {member}})

        client.set_iam_policy(table, policy)
        click.echo(f"  Re-applied sharing for {len(shared_with)} user(s)")
    except Exception as e:
        logging.warning(f"Could not re-apply sharing: {e}")


def auto_deploy_if_needed(
    query_file: Path,
    target_project: str,
):
    """Automatically deploy table schema, creating or updating as needed."""
    _, target_dataset, table_name = extract_from_query_path(query_file)

    client = bigquery.Client(project=target_project)
    dataset_ref = f"{target_project}.{target_dataset}"
    table_ref = f"{dataset_ref}.{table_name}"

    if not ensure_dataset_exists(client, dataset_ref):
        return

    try:
        deploy_table(artifact_file=query_file, destination_table=table_ref, force=True)
        click.echo(f"✅ Deployed: {table_ref}")
    except Exception as e:
        click.echo(f"⚠️  Deployment failed: {e}")
        return

    _reapply_shared_access(client, table_ref, query_file)


def prepare_target_files(
    query_files: List[Path],
    sql_dir: str,
    project_id: str,
    target: "Target",
    defer_to_target: bool,
    isolated: bool,
    auto_deploy: bool = True,
) -> List[Path]:
    """Prepare target directories for multiple query files."""
    copied_target_dirs: Set[Path] = set()
    target_query_files = [
        prepare_target_directory(
            query_file,
            sql_dir,
            target,
            defer_to_target,
            isolated,
            copied_target_dirs=copied_target_dirs,
        )
        for query_file in query_files
    ]

    if auto_deploy:
        for query_file in target_query_files:
            auto_deploy_if_needed(query_file, target.project_id or project_id)

    return target_query_files
