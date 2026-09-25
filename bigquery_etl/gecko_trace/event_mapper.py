"""Point new gecko trace signatures at existing stable ids.

Event signatures are hashes of (source_file, source_line, result), so every
Firefox release that moves a line produces a new signature for the same event.
Searchfox stores, for every line at every revision, the revision that
introduced the line and the position it had there. That triple does not
change while the line is not modified. Two events with the same triple and
the same result are the same event, and get the same stable_event_id.

The mapper runs once a day after the dimension tables and does four things:

1. Resolve the builds of events without cached blame to (Searchfox tree,
   git revision) through buildhub2 and the Searchfox ``hgrev`` redirect, and
   cache them in ``gecko_trace_source_revisions_v1``.
2. Fetch the blame of the source lines of those events at those revisions
   and cache them in ``gecko_trace_blame_lines_v1``. A cached
   (tree, git_rev, source_file, lineno) is never fetched again, so after the
   first run only new events and lines whose revision Searchfox has not
   indexed yet are pending.
3. Repoint ``stable_event_id`` in ``gecko_trace_events_v1`` where two events
   share the blame triple and the result. The oldest event keeps its id.
4. Recompute ``trace_key`` in ``gecko_trace_traces_v1`` from the ordered
   stable event ids and repoint ``stable_trace_id`` to the oldest trace with
   the same key.

Steps 3 and 4 are plain SQL over the cache tables and are idempotent.
"""

import datetime
import logging
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import bigquery

from bigquery_etl.gecko_trace import APPLICATIONS
from bigquery_etl.gecko_trace.searchfox import (
    BlameUnavailable,
    SearchfoxClient,
    normalize_source_path,
    tree_for_repository,
)

SOURCE_REVISIONS_SCHEMA = [
    bigquery.SchemaField("app_build", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("tree", "STRING"),
    bigquery.SchemaField("hg_rev", "STRING"),
    bigquery.SchemaField("git_rev", "STRING"),
    bigquery.SchemaField("resolved_date", "DATE"),
]

BLAME_LINES_SCHEMA = [
    bigquery.SchemaField("tree", "STRING"),
    bigquery.SchemaField("git_rev", "STRING"),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("lineno", "INT64"),
    bigquery.SchemaField("origin_rev", "STRING"),
    bigquery.SchemaField("origin_path", "STRING"),
    bigquery.SchemaField("origin_lineno", "INT64"),
    bigquery.SchemaField("fetched_date", "DATE"),
]

# (build, source file, line) triples of events that have no cached blame for
# the build's revision. Selecting on coverage rather than on date makes the
# first run backfill every existing event and later runs retry lines whose
# revision Searchfox had not indexed yet. revision_known tells whether the
# build was already looked up; a known build with a NULL git_rev is
# unresolvable and is skipped.
PENDING_QUERY = """
WITH event_builds AS (
  SELECT DISTINCT
    e.source_file,
    e.source_line,
    pc.app_build
  FROM
    `{project}.{app_id}_derived.gecko_trace_events_v1` e
  JOIN
    `{project}.{app_id}_derived.gecko_trace_trace_events_v1` te
    USING (event_signature)
  JOIN
    `{project}.{app_id}_derived.gecko_trace_platform_counts_v1` pc
    USING (trace_signature)
  WHERE
    e.source_line IS NOT NULL
    AND pc.app_build IS NOT NULL
)
SELECT
  eb.app_build,
  eb.source_file,
  eb.source_line,
  sr.app_build IS NOT NULL AS revision_known,
  sr.tree,
  sr.git_rev
FROM
  event_builds eb
LEFT JOIN
  `{project}.{app_id}_derived.gecko_trace_source_revisions_v1` sr
  USING (app_build)
LEFT JOIN
  `{project}.{app_id}_derived.gecko_trace_blame_lines_v1` bl
  ON bl.tree = sr.tree
  AND bl.git_rev = sr.git_rev
  AND bl.source_file = eb.source_file
  AND bl.lineno = eb.source_line
WHERE
  bl.source_file IS NULL
ORDER BY
  eb.app_build,
  eb.source_file,
  eb.source_line
"""

BUILDHUB_QUERY = """
SELECT
  build.build.id AS app_build,
  ANY_VALUE(build.target.channel) AS channel,
  ANY_VALUE(build.source.revision) AS hg_rev,
  ANY_VALUE(build.source.repository) AS repository
FROM
  `{project}.telemetry.buildhub2`
WHERE
  build.build.id IN UNNEST(@builds)
GROUP BY
  app_build
"""

# Give every event the stable id of the oldest event that has the same blame
# triple and the same result. An event seen in several revisions can carry
# several triples; the first candidate id in (first_seen_date, id) order wins.
EVENT_MAPPING_MERGE = """
MERGE INTO
  `{project}.{app_id}_derived.gecko_trace_events_v1` AS T
  USING (
    WITH event_revs AS (
      SELECT DISTINCT
        te.event_signature,
        sr.tree,
        sr.git_rev
      FROM
        `{project}.{app_id}_derived.gecko_trace_trace_events_v1` te
      JOIN
        `{project}.{app_id}_derived.gecko_trace_platform_counts_v1` pc
        USING (trace_signature)
      JOIN
        `{project}.{app_id}_derived.gecko_trace_source_revisions_v1` sr
        USING (app_build)
      WHERE
        sr.git_rev IS NOT NULL
    ),
    identities AS (
      SELECT
        e.event_signature,
        e.stable_event_id,
        e.result,
        e.first_seen_date,
        bl.origin_rev,
        bl.origin_path,
        bl.origin_lineno
      FROM
        `{project}.{app_id}_derived.gecko_trace_events_v1` e
      JOIN
        event_revs er
        USING (event_signature)
      JOIN
        `{project}.{app_id}_derived.gecko_trace_blame_lines_v1` bl
        ON bl.tree = er.tree
        AND bl.git_rev = er.git_rev
        AND bl.source_file = e.source_file
        AND bl.lineno = e.source_line
      WHERE
        bl.origin_rev IS NOT NULL
    ),
    canonical AS (
      SELECT
        origin_rev,
        origin_path,
        origin_lineno,
        result,
        ARRAY_AGG(stable_event_id ORDER BY first_seen_date, stable_event_id LIMIT 1)[
          OFFSET(0)
        ] AS stable_event_id,
        MIN(first_seen_date) AS first_seen_date
      FROM
        identities
      GROUP BY
        origin_rev,
        origin_path,
        origin_lineno,
        result
    ),
    mapped AS (
      SELECT
        i.event_signature,
        ARRAY_AGG(c.stable_event_id ORDER BY c.first_seen_date, c.stable_event_id LIMIT 1)[
          OFFSET(0)
        ] AS stable_event_id
      FROM
        identities i
      JOIN
        canonical c
        USING (origin_rev, origin_path, origin_lineno, result)
      GROUP BY
        i.event_signature
    )
    SELECT
      m.event_signature,
      m.stable_event_id
    FROM
      mapped m
    JOIN
      `{project}.{app_id}_derived.gecko_trace_events_v1` e
      USING (event_signature)
    WHERE
      e.stable_event_id != m.stable_event_id
  ) AS S
  ON T.event_signature = S.event_signature
WHEN MATCHED
THEN
  UPDATE
  SET
    stable_event_id = S.stable_event_id
"""

# Recompute trace keys from the bridge and the current stable event ids, and
# give every trace the stable id of the oldest trace with the same key.
TRACE_MAPPING_MERGE = """
MERGE INTO
  `{project}.{app_id}_derived.gecko_trace_traces_v1` AS T
  USING (
    WITH trace_keys AS (
      SELECT
        te.trace_signature,
        TO_BASE64(SHA256(STRING_AGG(e.stable_event_id, ',' ORDER BY te.event_position))) AS trace_key
      FROM
        `{project}.{app_id}_derived.gecko_trace_trace_events_v1` te
      LEFT JOIN
        `{project}.{app_id}_derived.gecko_trace_events_v1` e
        USING (event_signature)
      GROUP BY
        te.trace_signature
      HAVING
        COUNTIF(e.stable_event_id IS NULL) = 0
    ),
    canonical AS (
      SELECT
        tk.trace_key,
        ARRAY_AGG(t.stable_trace_id ORDER BY t.first_seen_date, t.stable_trace_id LIMIT 1)[
          OFFSET(0)
        ] AS stable_trace_id
      FROM
        trace_keys tk
      JOIN
        `{project}.{app_id}_derived.gecko_trace_traces_v1` t
        USING (trace_signature)
      GROUP BY
        tk.trace_key
    )
    SELECT
      t.trace_signature,
      tk.trace_key,
      c.stable_trace_id
    FROM
      `{project}.{app_id}_derived.gecko_trace_traces_v1` t
    JOIN
      trace_keys tk
      USING (trace_signature)
    JOIN
      canonical c
      USING (trace_key)
    WHERE
      t.trace_key != tk.trace_key
      OR t.stable_trace_id != c.stable_trace_id
  ) AS S
  ON T.trace_signature = S.trace_signature
WHEN MATCHED
THEN
  UPDATE
  SET
    trace_key = S.trace_key,
    stable_trace_id = S.stable_trace_id
"""


class EventMapper:
    """Map new event and trace signatures to existing stable ids."""

    def __init__(
        self,
        searchfox: SearchfoxClient,
        project: str = "moz-fx-data-shared-prod",
        bq_client=None,
    ):
        """Set up the Searchfox and BigQuery clients."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.project = project
        self.searchfox = searchfox
        self.bq = bq_client or bigquery.Client(project=project)

    def _query(self, template: str, app_id: Optional[str] = None, **params):
        sql = template.format(project=self.project, app_id=app_id)
        query_parameters: list = []
        for name, value in params.items():
            if isinstance(value, datetime.date):
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "DATE", value)
                )
            elif isinstance(value, int):
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "INT64", value)
                )
            elif isinstance(value, (list, tuple)):
                query_parameters.append(
                    bigquery.ArrayQueryParameter(name, "STRING", list(value))
                )
            else:
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                )
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        job = self.bq.query(sql, job_config=job_config)
        rows = list(job.result())
        return rows, job

    def _load(self, app_id: str, table: str, schema, rows: List[dict]):
        if not rows:
            return
        destination = f"{self.project}.{app_id}_derived.{table}"
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        self.bq.load_table_from_json(rows, destination, job_config=job_config).result()
        self.logger.info("Loaded %d rows into %s", len(rows), destination)

    def _resolve_builds(
        self, app_id: str, builds: Set[str], reference_date: datetime.date
    ) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """Look unknown builds up in buildhub2 and Searchfox, and cache them."""
        resolved: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        if not builds:
            return resolved
        rows, _ = self._query(BUILDHUB_QUERY, builds=sorted(builds))
        found = {row.app_build: row for row in rows}
        cache_rows = []
        for app_build in sorted(builds):
            row = found.get(app_build)
            channel = hg_rev = tree = git_rev = None
            if row is not None:
                channel, hg_rev = row.channel, row.hg_rev
                tree = tree_for_repository(row.repository)
                if tree and hg_rev:
                    git_rev = self.searchfox.resolve_hg_rev(tree, hg_rev)
            if git_rev is None:
                self.logger.warning(
                    "Build %s of %s cannot be resolved (buildhub row: %s, tree: %s)",
                    app_build,
                    app_id,
                    row is not None,
                    tree,
                )
            resolved[app_build] = (tree, git_rev)
            cache_rows.append(
                dict(
                    app_build=app_build,
                    channel=channel,
                    tree=tree,
                    hg_rev=hg_rev,
                    git_rev=git_rev,
                    resolved_date=reference_date.isoformat(),
                )
            )
        self._load(
            app_id,
            "gecko_trace_source_revisions_v1",
            SOURCE_REVISIONS_SCHEMA,
            cache_rows,
        )
        return resolved

    def _fetch_blame(
        self,
        app_id: str,
        wanted: Dict[Tuple[str, str], Dict[str, Set[int]]],
        reference_date: datetime.date,
    ) -> int:
        """Fetch blame for the wanted (tree, git_rev) -> source file -> lines."""
        cache_rows: List[dict] = []
        for (tree, git_rev), files in sorted(wanted.items()):
            # Several event paths can normalize to one repository path.
            by_path: Dict[str, Dict[str, Set[int]]] = {}
            for source_file, lines in files.items():
                by_path.setdefault(normalize_source_path(source_file), {})[
                    source_file
                ] = lines
            selectors = {
                path: set().union(*files.values()) for path, files in by_path.items()
            }
            try:
                blame = self.searchfox.blame_lines(tree, git_rev, selectors)
            except BlameUnavailable as error:
                # Not cached, so the same lines are retried on the next run.
                self.logger.warning("No blame yet, will retry: %s", error)
                continue
            for path, entries in blame.items():
                origins = {
                    entry["line"]: entry for entry in entries or [] if entry["line"]
                }
                for source_file, lines in by_path.get(path, {}).items():
                    for line in sorted(lines):
                        # A missing file or a line past its end is cached with
                        # NULL origin fields, so it is not asked for again.
                        origin = origins.get(line, {})
                        cache_rows.append(
                            dict(
                                tree=tree,
                                git_rev=git_rev,
                                source_file=source_file,
                                lineno=line,
                                origin_rev=origin.get("rev"),
                                origin_path=origin.get("path"),
                                origin_lineno=origin.get("lineno"),
                                fetched_date=reference_date.isoformat(),
                            )
                        )
        self._load(app_id, "gecko_trace_blame_lines_v1", BLAME_LINES_SCHEMA, cache_rows)
        return len(cache_rows)

    def update_cache(self, app_id: str, reference_date: datetime.date):
        """Resolve builds and fetch blame for event lines without cached blame."""
        pending, _ = self._query(PENDING_QUERY, app_id=app_id)
        unknown_builds = {row.app_build for row in pending if not row.revision_known}
        resolved = self._resolve_builds(app_id, unknown_builds, reference_date)

        wanted: Dict[Tuple[str, str], Dict[str, Set[int]]] = {}
        for row in pending:
            if row.revision_known:
                tree, git_rev = row.tree, row.git_rev
            else:
                tree, git_rev = resolved[row.app_build]
            if tree and git_rev:
                wanted.setdefault((tree, git_rev), {}).setdefault(
                    row.source_file, set()
                ).add(row.source_line)
        fetched = self._fetch_blame(app_id, wanted, reference_date)
        self.logger.info(
            "%s: %d pending (build, file, line) triples, %d builds resolved, "
            "%d revisions fetched, %d blame rows cached",
            app_id,
            len(pending),
            len(unknown_builds),
            len(wanted),
            fetched,
        )

    def remap(self, app_id: str) -> Tuple[int, int]:
        """Repoint stable event ids and stable trace ids. Returns the row counts."""
        _, events_job = self._query(EVENT_MAPPING_MERGE, app_id=app_id)
        events = events_job.num_dml_affected_rows or 0
        _, traces_job = self._query(TRACE_MAPPING_MERGE, app_id=app_id)
        traces = traces_job.num_dml_affected_rows or 0
        self.logger.info(
            "%s: repointed %d events and %d traces", app_id, events, traces
        )
        return events, traces

    def run(
        self,
        reference_date: Optional[datetime.date] = None,
        applications=APPLICATIONS,
    ):
        """Update the caches and remap ids for every application.

        reference_date is only recorded on the cache rows as the fetch date.
        """
        if reference_date is None:
            reference_date = datetime.date.today()
        for app_id in applications:
            self.update_cache(app_id, reference_date)
            self.remap(app_id)
