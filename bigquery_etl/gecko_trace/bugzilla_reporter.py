"""File Bugzilla bugs for new gecko trace patterns."""

import datetime
import logging
from itertools import groupby

import bugzilla
from google.cloud import bigquery

BUGZILLA_PROD_URL = "https://bugzilla.mozilla.org/rest/"
BUGZILLA_DEV_URL = "https://bugzilla-dev.allizom.org/rest/"
PRODUCT = "Firefox"
COMPONENT = "General"

APPLICATIONS = (
    "firefox_desktop",
    "org_mozilla_fenix_nightly",
    "org_mozilla_firefox_beta",
)

BQ_NEW_TRACES_QUERY = """
  SELECT
    t.stable_trace_id,
    e.source_file,
    e.source_line,
    e.result,
    te.event_position
  FROM `{project}.gecko_trace_aggregates.traces` t
  JOIN `{project}.{app_id}_derived.gecko_trace_trace_events_v1` te
    ON t.stable_trace_id = te.stable_trace_id
  JOIN `{project}.{app_id}_derived.gecko_trace_events_v1` e
    ON te.stable_event_id = e.stable_event_id
  LEFT JOIN `{project}.{app_id}_derived.gecko_trace_bug_reports_v1` br
    ON t.stable_trace_id = br.stable_trace_id
  WHERE
    t.app_id = @app_id
    AND t.latest_seen_date >= DATE_SUB(@reference_date, INTERVAL 7 DAY)
    AND t.first_seen_date >= DATE_SUB(@reference_date, INTERVAL 7 DAY)
    AND br.stable_trace_id IS NULL
  ORDER BY t.stable_trace_id, te.event_position
"""

BQ_PLATFORM_QUERY = """
  SELECT
    app_build,
    normalized_os,
    normalized_os_version,
    architecture,
    SUM(hit_count) AS hit_count
  FROM `{project}.{app_id}_derived.gecko_trace_platform_counts_v1`
  WHERE stable_trace_id = @stable_trace_id
  GROUP BY app_build, normalized_os, normalized_os_version, architecture
  ORDER BY hit_count DESC
  LIMIT 10
"""

BQ_RECORD_BUG_QUERY = """
  INSERT INTO `{table}`
    (submission_date, stable_trace_id, app_id, bug_id, filed_date)
  VALUES (@submission_date, @stable_trace_id, @app_id, @bug_id, @filed_date)
"""


class BugzillaReporter:
    """File Bugzilla bugs for new gecko trace patterns."""

    def __init__(
        self,
        api_key,
        project="moz-fx-data-shared-prod",
        dev=False,
    ):
        """Set up the Bugzilla and BigQuery clients."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.project = project
        self.bq_client = bigquery.Client(project=project)
        api_url = BUGZILLA_DEV_URL if dev else BUGZILLA_PROD_URL
        self.bz_client = bugzilla.Bugzilla(url=api_url, api_key=api_key)
        if dev:
            self.logger.info("Running against Bugzilla dev instance")

    def _format_events(self, events):
        """Build a plain-text event list with searchfox links."""
        lines = []
        for event in events:
            url = (
                f"https://searchfox.org/firefox-main/source/"
                f"{event.source_file}#{event.source_line}"
            )
            lines.append(f"  {url}  result={event.result}")
        return "\n".join(lines)

    def _format_platforms(self, platforms):
        """Build a plain-text list of affected platforms."""
        if not platforms:
            return "  (no platform data available)"
        lines = []
        for p in platforms:
            lines.append(
                f"  {p.normalized_os} {p.normalized_os_version} / "
                f"{p.architecture} (build {p.app_build}) "
                f"-- {p.hit_count} hits"
            )
        return "\n".join(lines)

    def _get_platforms(self, app_id, stable_trace_id):
        """Get platform counts for a trace."""
        query = BQ_PLATFORM_QUERY.format(
            project=self.project, app_id=app_id
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "stable_trace_id", "STRING", stable_trace_id
                ),
            ]
        )
        try:
            return list(
                self.bq_client.query(query, job_config=job_config).result()
            )
        except Exception:
            self.logger.exception(
                "Failed to get platform data for %s", trace_signature
            )
            return []

    def _record_bug(self, stable_trace_id, app_id, bug_id, reference_date):
        """Record the bug in the mapping table."""
        table = f"{self.project}.{app_id}_derived.gecko_trace_bug_reports_v1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "submission_date", "DATE", reference_date
                ),
                bigquery.ScalarQueryParameter(
                    "stable_trace_id", "STRING", stable_trace_id
                ),
                bigquery.ScalarQueryParameter("app_id", "STRING", app_id),
                bigquery.ScalarQueryParameter("bug_id", "INT64", bug_id),
                bigquery.ScalarQueryParameter(
                    "filed_date", "DATE", reference_date
                ),
            ]
        )
        self.bq_client.query(
            BQ_RECORD_BUG_QUERY.format(table=table), job_config=job_config
        ).result()
        self.logger.info(
            "Recorded bug %d for trace %s in %s", bug_id, stable_trace_id, app_id
        )

    def _file_bug(self, stable_trace_id, app_id, events, platforms):
        """File one Bugzilla bug and return the bug ID, or None on failure."""
        bug = bugzilla.DotDict()
        bug.product = PRODUCT
        bug.component = COMPONENT
        bug.summary = (
            f"[gecko-trace] New trace pattern {stable_trace_id[:12]} in {app_id}"
        )
        bug.description = (
            f"A new trace pattern was detected in {app_id}.\n"
            f"Trace ID: {stable_trace_id}\n\n"
            f"Platforms affected:\n{self._format_platforms(platforms)}\n\n"
            f"Events in execution order:\n{self._format_events(events)}"
        )
        bug["type"] = "defect"
        bug.version = "unspecified"

        try:
            result = self.bz_client.post_bug(bug)
            bug_id = result["id"]
            self.logger.info("Filed bug %d for trace %s", bug_id, stable_trace_id)
            return bug_id
        except Exception:
            self.logger.exception(
                "Failed to file bug for trace %s in %s", stable_trace_id, app_id
            )
            return None

    def report_new_traces(self, reference_date=None):
        """Find new traces and file Bugzilla bugs.

        Args:
            reference_date: The date to use for the 7-day window.
                Defaults to today. Pass a specific date for backfills.
        """
        if reference_date is None:
            reference_date = datetime.date.today()

        filed = 0
        failed = 0

        for app_id in APPLICATIONS:
            self.logger.info("Looking for new traces in %s", app_id)
            query = BQ_NEW_TRACES_QUERY.format(
                project=self.project, app_id=app_id
            )
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("app_id", "STRING", app_id),
                    bigquery.ScalarQueryParameter(
                        "reference_date", "DATE", reference_date
                    ),
                ]
            )
            rows = list(self.bq_client.query(query, job_config=job_config).result())

            for stable_trace_id, events_iter in groupby(
                rows, key=lambda r: r.stable_trace_id
            ):
                events = list(events_iter)
                self.logger.info(
                    "Trace %s has %d events", stable_trace_id, len(events)
                )

                platforms = self._get_platforms(app_id, stable_trace_id)

                bug_id = self._file_bug(
                    stable_trace_id, app_id, events, platforms
                )
                if bug_id is None:
                    failed += 1
                    continue

                try:
                    self._record_bug(
                        stable_trace_id, app_id, bug_id, reference_date
                    )
                    filed += 1
                except Exception:
                    self.logger.exception(
                        "Filed bug %d but failed to record it for trace %s. "
                        "This trace may get a duplicate bug on the next run.",
                        bug_id,
                        stable_trace_id,
                    )
                    failed += 1

        self.logger.info("Done. Filed %d bugs, %d failures.", filed, failed)
        if failed > 0:
            raise RuntimeError(
                f"{failed} trace(s) failed to file or record. "
                "Check the logs and re-run to retry."
            )
