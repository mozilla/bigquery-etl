"""File and update Bugzilla bugs for gecko trace patterns."""

import datetime
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import requests
from google.cloud import bigquery

from bigquery_etl.gecko_trace import APPLICATIONS

BUGZILLA_PROD_URL = "https://bugzilla.mozilla.org/rest"
BUGZILLA_DEV_URL = "https://bugzilla-dev.allizom.org/rest"
PRODUCT = "Firefox"
COMPONENT = "General"
WINDOW_DAYS = 7
TOP_PLATFORMS = 10
SEARCHFOX_URL = "https://searchfox.org/firefox-main/source/{file}#{line}"


NEW_TRACES_QUERY = """
SELECT
  stable_trace_id
FROM
  `{project}.gecko_trace_aggregates.traces`
WHERE
  app_id = @app_id
  AND stable_trace_id NOT IN (
    SELECT stable_trace_id
    FROM `{project}.gecko_trace_aggregates.bug_reports`
    WHERE app_id = @app_id
  )
GROUP BY
  stable_trace_id
HAVING
  MIN(first_seen_date) > DATE_SUB(@reference_date, INTERVAL @window_days DAY)
ORDER BY
  stable_trace_id
"""

KNOWN_ACTIVE_TRACES_QUERY = """
SELECT
  br.stable_trace_id,
  br.bug_id,
  br.last_comment_date,
  br.last_reported_trace_signature
FROM
  `{project}.gecko_trace_aggregates.bug_reports` br
JOIN
  `{project}.gecko_trace_aggregates.traces` t
  ON br.app_id = t.app_id
  AND br.stable_trace_id = t.stable_trace_id
WHERE
  br.app_id = @app_id
GROUP BY
  br.stable_trace_id,
  br.bug_id,
  br.last_comment_date,
  br.last_reported_trace_signature
HAVING
  MAX(t.last_seen_date) > DATE_SUB(@reference_date, INTERVAL @window_days DAY)
ORDER BY
  br.stable_trace_id
"""

COUNTS_QUERY = """
SELECT
  SUM(hit_count) AS hit_count,
  SAFE_DIVIDE(SUM(avg_duration_nano * hit_count), SUM(hit_count)) AS avg_duration_nano,
  MIN(submission_date) AS first_date,
  MAX(submission_date) AS last_date
FROM
  `{project}.gecko_trace_aggregates.traces_daily`
WHERE
  app_id = @app_id
  AND stable_trace_id = @stable_trace_id
  AND submission_date > DATE_SUB(@reference_date, INTERVAL @window_days DAY)
"""

PLATFORMS_QUERY = """
SELECT
  app_build,
  normalized_os,
  normalized_os_version,
  architecture,
  SUM(hit_count) AS hit_count
FROM
  `{project}.gecko_trace_aggregates.platform_counts`
WHERE
  app_id = @app_id
  AND stable_trace_id = @stable_trace_id
  AND submission_date > DATE_SUB(@reference_date, INTERVAL @window_days DAY)
GROUP BY
  app_build,
  normalized_os,
  normalized_os_version,
  architecture
ORDER BY
  hit_count DESC
LIMIT
  {top_platforms}
"""

CURRENT_SIGNATURE_QUERY = """
SELECT
  trace_signature
FROM
  `{project}.gecko_trace_aggregates.traces`
WHERE
  app_id = @app_id
  AND stable_trace_id = @stable_trace_id
ORDER BY
  last_seen_date DESC,
  trace_signature
LIMIT
  1
"""

EVENTS_QUERY = """
SELECT
  event_position,
  source_file,
  source_line,
  result
FROM
  `{project}.gecko_trace_aggregates.trace_events`
WHERE
  app_id = @app_id
  AND trace_signature = @trace_signature
ORDER BY
  event_position
"""

INSERT_BUG_REPORT_QUERY = """
INSERT INTO
  `{project}.{app_id}_derived.gecko_trace_bug_reports_v1`
  (stable_trace_id, bug_id, filed_date, last_comment_date, last_reported_trace_signature)
VALUES
  (@stable_trace_id, @bug_id, @reference_date, @reference_date, @trace_signature)
"""

UPDATE_BUG_REPORT_QUERY = """
UPDATE
  `{project}.{app_id}_derived.gecko_trace_bug_reports_v1`
SET
  last_comment_date = @reference_date,
  last_reported_trace_signature = @trace_signature
WHERE
  stable_trace_id = @stable_trace_id
"""


@dataclass
class TraceStats:
    """Everything the reporter says about one trace pattern."""

    stable_trace_id: str
    app_id: str
    trace_signature: Optional[str]
    hit_count: int
    avg_duration_nano: Optional[float]
    first_date: Optional[datetime.date]
    last_date: Optional[datetime.date]
    platforms: List = field(default_factory=list)
    events: List = field(default_factory=list)


class BugzillaClient:
    """Minimal Bugzilla REST client."""

    def __init__(self, api_key, dev=False, timeout=30):
        """Set the API key and the Bugzilla instance."""
        self.base_url = BUGZILLA_DEV_URL if dev else BUGZILLA_PROD_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["X-BUGZILLA-API-KEY"] = api_key

    def _request(self, method, path, **kwargs):
        response = self.session.request(
            method, f"{self.base_url}/{path}", timeout=self.timeout, **kwargs
        )
        response.raise_for_status()
        return response.json()

    def file_bug(self, summary, description):
        """Create a bug and return its id."""
        payload = {
            "product": PRODUCT,
            "component": COMPONENT,
            "summary": summary,
            "description": description,
            "version": "unspecified",
            "type": "defect",
        }
        return self._request("POST", "bug", json=payload)["id"]

    def get_status(self, bug_id):
        """Return (status, resolution) of a bug."""
        data = self._request(
            "GET", f"bug/{bug_id}", params={"include_fields": "status,resolution"}
        )
        bug = data["bugs"][0]
        return bug.get("status"), bug.get("resolution")

    def add_comment(self, bug_id, text):
        """Post a comment to a bug."""
        self._request("POST", f"bug/{bug_id}/comment", json={"comment": text})


class BugzillaReporter:
    """File bugs for new trace patterns and update bugs of active ones."""

    def __init__(self, bugzilla, project="moz-fx-data-shared-prod", bq_client=None):
        """Set up the Bugzilla and BigQuery clients."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.project = project
        self.bugzilla = bugzilla
        self.bq = bq_client or bigquery.Client(project=project)

    def _query(self, template, **params):
        sql = template.format(
            project=self.project,
            top_platforms=TOP_PLATFORMS,
            **{k: v for k, v in params.items() if k == "app_id"},
        )
        query_parameters = []
        for name, value in params.items():
            if isinstance(value, datetime.date):
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "DATE", value)
                )
            elif isinstance(value, int):
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "INT64", value)
                )
            else:
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                )
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        return list(self.bq.query(sql, job_config=job_config).result())

    def _collect_stats(self, app_id, stable_trace_id, reference_date):
        base = dict(
            app_id=app_id,
            stable_trace_id=stable_trace_id,
            reference_date=reference_date,
            window_days=WINDOW_DAYS,
        )
        counts = self._query(COUNTS_QUERY, **base)[0]
        platforms = self._query(PLATFORMS_QUERY, **base)
        signatures = self._query(
            CURRENT_SIGNATURE_QUERY, app_id=app_id, stable_trace_id=stable_trace_id
        )
        trace_signature = signatures[0].trace_signature if signatures else None
        events = (
            self._query(EVENTS_QUERY, app_id=app_id, trace_signature=trace_signature)
            if trace_signature
            else []
        )
        return TraceStats(
            stable_trace_id=stable_trace_id,
            app_id=app_id,
            trace_signature=trace_signature,
            hit_count=counts.hit_count or 0,
            avg_duration_nano=counts.avg_duration_nano,
            first_date=counts.first_date,
            last_date=counts.last_date,
            platforms=platforms,
            events=events,
        )

    @staticmethod
    def format_stats(stats, reference_date, signature_changed=False):
        """Render the plain-text stats block used in bug descriptions and comments."""
        window_start = reference_date - datetime.timedelta(days=WINDOW_DAYS - 1)
        lines = [
            f"Trace ID: {stats.stable_trace_id}",
            f"Application: {stats.app_id}",
            f"Window: {window_start} to {reference_date}",
            f"Occurrences: {stats.hit_count}",
        ]
        if stats.avg_duration_nano is not None:
            lines.append(f"Average duration: {stats.avg_duration_nano / 1e6:.2f} ms")
        if stats.first_date and stats.last_date:
            lines.append(f"Seen from {stats.first_date} to {stats.last_date}")
        lines.append("")
        lines.append(f"Platforms (top {TOP_PLATFORMS} by occurrences):")
        if stats.platforms:
            for p in stats.platforms:
                lines.append(
                    f"  {p.normalized_os} {p.normalized_os_version} / {p.architecture}"
                    f" (build {p.app_build}) -- {p.hit_count} hits"
                )
        else:
            lines.append("  (no platform data available)")
        lines.append("")
        header = "Events in execution order"
        if signature_changed:
            header += " (source locations changed since the last report)"
        lines.append(f"{header}:")
        if stats.events:
            for e in stats.events:
                url = SEARCHFOX_URL.format(file=e.source_file, line=e.source_line)
                lines.append(f"  {e.event_position}. {url}  result={e.result}")
        else:
            lines.append("  (no event data available)")
        return "\n".join(lines)

    def _file_new_bug(self, app_id, stable_trace_id, reference_date):
        stats = self._collect_stats(app_id, stable_trace_id, reference_date)
        summary = f"[gecko-trace] New trace pattern {stable_trace_id[:12]} in {app_id}"
        description = (
            f"A new trace pattern was detected in {app_id}.\n\n"
            + self.format_stats(stats, reference_date)
        )
        bug_id = self.bugzilla.file_bug(summary, description)
        self.logger.info("Filed bug %s for trace %s", bug_id, stable_trace_id)
        try:
            self._query(
                INSERT_BUG_REPORT_QUERY,
                app_id=app_id,
                stable_trace_id=stable_trace_id,
                bug_id=bug_id,
                reference_date=reference_date,
                trace_signature=stats.trace_signature,
            )
        except Exception:
            self.logger.exception(
                "Filed bug %s but failed to record it for trace %s. "
                "This trace may get a duplicate bug on the next run.",
                bug_id,
                stable_trace_id,
            )
            raise

    def _update_known_bug(self, app_id, known, reference_date):
        if (
            known.last_comment_date is not None
            and (reference_date - known.last_comment_date).days < WINDOW_DAYS
        ):
            self.logger.info(
                "Skipping bug %s for trace %s: last comment on %s",
                known.bug_id,
                known.stable_trace_id,
                known.last_comment_date,
            )
            return
        stats = self._collect_stats(app_id, known.stable_trace_id, reference_date)
        status, resolution = self.bugzilla.get_status(known.bug_id)
        signature_changed = (
            stats.trace_signature is not None
            and stats.trace_signature != known.last_reported_trace_signature
        )
        comment = "Weekly gecko-trace update.\n\n" + self.format_stats(
            stats, reference_date, signature_changed=signature_changed
        )
        if status == "RESOLVED":
            comment += (
                f"\n\nThis bug is RESOLVED {resolution or ''}".rstrip()
                + ", but the trace pattern is still seen."
            )
        self.bugzilla.add_comment(known.bug_id, comment)
        self.logger.info(
            "Commented on bug %s (%s) for trace %s",
            known.bug_id,
            status,
            known.stable_trace_id,
        )
        self._query(
            UPDATE_BUG_REPORT_QUERY,
            app_id=app_id,
            stable_trace_id=known.stable_trace_id,
            reference_date=reference_date,
            trace_signature=stats.trace_signature,
        )

    def report(self, reference_date=None):
        """File bugs for new traces and comment on bugs of active known traces.

        Args:
            reference_date: Last day of the 7-day window. Defaults to today.
        """
        if reference_date is None:
            reference_date = datetime.date.today()

        filed = commented = failed = 0
        for app_id in APPLICATIONS:
            window = dict(
                app_id=app_id, reference_date=reference_date, window_days=WINDOW_DAYS
            )

            for row in self._query(NEW_TRACES_QUERY, **window):
                try:
                    self._file_new_bug(app_id, row.stable_trace_id, reference_date)
                    filed += 1
                except Exception:
                    self.logger.exception(
                        "Failed to file a bug for trace %s in %s",
                        row.stable_trace_id,
                        app_id,
                    )
                    failed += 1

            for known in self._query(KNOWN_ACTIVE_TRACES_QUERY, **window):
                try:
                    self._update_known_bug(app_id, known, reference_date)
                    commented += 1
                except Exception:
                    self.logger.exception(
                        "Failed to update bug %s for trace %s in %s",
                        known.bug_id,
                        known.stable_trace_id,
                        app_id,
                    )
                    failed += 1

        self.logger.info(
            "Done. Filed %d bugs, updated %d bugs, %d failures.",
            filed,
            commented,
            failed,
        )
        if failed:
            raise RuntimeError(
                f"{failed} trace(s) failed to file or update. Check the logs and re-run to retry."
            )
