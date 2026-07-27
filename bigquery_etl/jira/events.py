"""Fetch Jira ticket change events and load them into BigQuery."""

import json
import logging
import tempfile
from datetime import date
from typing import Callable, Iterable, Iterator, Optional

from google.cloud import bigquery

EVENT_SCHEMA = [
    bigquery.SchemaField("issue_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("project_key", "STRING"),
    bigquery.SchemaField("issue_type", "STRING"),
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("actor_id", "STRING"),
    bigquery.SchemaField("actor_name", "STRING"),
    bigquery.SchemaField("field", "STRING"),
    bigquery.SchemaField("field_type", "STRING"),
    bigquery.SchemaField("from_value", "STRING"),
    bigquery.SchemaField("to_value", "STRING"),
    bigquery.SchemaField("from_id", "STRING"),
    bigquery.SchemaField("to_id", "STRING"),
    bigquery.SchemaField("comment_is_public", "BOOL"),
]

# Jira writes a synthetic "Last Comment" changelog entry on every comment, which
# would both double-count `commented` events and smuggle full comment text into
# from_value/to_value. "Rank" is backlog-reordering noise.
EXCLUDED_CHANGELOG_FIELDS = frozenset({"Last Comment", "Rank"})

SEARCH_FIELDS = ["project", "issuetype", "created", "reporter"]

CHANGELOG_ITEMS_KEY = "values"
PAGE_SIZE = 100

logger = logging.getLogger(__name__)


def _event(
    issue: dict,
    *,
    event_id: str,
    event_type: str,
    event_ts: str,
    actor: Optional[dict] = None,
    field: Optional[str] = None,
    field_type: Optional[str] = None,
    from_value: Optional[str] = None,
    to_value: Optional[str] = None,
    from_id: Optional[str] = None,
    to_id: Optional[str] = None,
    comment_is_public: Optional[bool] = None,
) -> dict:
    """Build a row carrying every EVENT_SCHEMA column, defaulting to None."""
    actor = actor or {}
    return {
        "issue_key": issue["issue_key"],
        "project_key": issue["project_key"],
        "issue_type": issue["issue_type"],
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": event_ts,
        "actor_id": actor.get("accountId"),
        "actor_name": actor.get("displayName"),
        "field": field,
        "field_type": field_type,
        "from_value": from_value,
        "to_value": to_value,
        "from_id": from_id,
        "to_id": to_id,
        "comment_is_public": comment_is_public,
    }


def created_event(issue: dict) -> Optional[dict]:
    """Build the synthetic `created` event from the issue's own fields."""
    event_ts = issue.get("created")
    if not event_ts:
        logger.warning(
            "Skipping created event for %s: no created timestamp", issue["issue_key"]
        )
        return None

    return _event(
        issue,
        event_id="created",
        event_type="created",
        event_ts=event_ts,
        actor=issue.get("reporter"),
    )


def changelog_events(issue: dict, history: dict, to_ts: Callable) -> Iterator[dict]:
    """Yield one event per changelog item, excluding denied fields."""
    event_ts = to_ts(history.get("created"))
    if not event_ts:
        logger.warning(
            "Skipping changelog %s on %s: unparseable timestamp %r",
            history.get("id"),
            issue["issue_key"],
            history.get("created"),
        )
        return

    author = history.get("author") or {}

    for item in history.get("items") or []:
        field = item.get("field")
        if field in EXCLUDED_CHANGELOG_FIELDS:
            continue

        yield _event(
            issue,
            event_id=str(history.get("id")),
            event_type="status_change" if field == "status" else "field_change",
            event_ts=event_ts,
            actor=author,
            field=field,
            field_type=item.get("fieldtype"),
            from_value=item.get("fromString"),
            to_value=item.get("toString"),
            from_id=item.get("from"),
            to_id=item.get("to"),
        )


def _comment_is_public(comment: dict) -> bool:
    """Report whether a comment is visible to all project users."""
    if "jsdPublic" in comment:
        return bool(comment["jsdPublic"])
    return "visibility" not in comment


def comment_event(issue: dict, comment: dict, to_ts: Callable) -> Optional[dict]:
    """Build a `commented` event from a comment payload, without its body."""
    event_ts = to_ts(comment.get("created"))
    if not event_ts:
        logger.warning(
            "Skipping comment %s on %s: unparseable timestamp %r",
            comment.get("id"),
            issue["issue_key"],
            comment.get("created"),
        )
        return None

    return _event(
        issue,
        event_id=str(comment.get("id")),
        event_type="commented",
        event_ts=event_ts,
        actor=comment.get("author"),
        comment_is_public=_comment_is_public(comment),
    )


class JiraEventsAPI:
    """Read issue change events from the Jira REST API."""

    SEARCH_ENDPOINT = "/rest/api/3/search/jql"
    CHANGELOG_ENDPOINT = "/rest/api/3/issue/{issue_key}/changelog"
    COMMENT_ENDPOINT = "/rest/api/3/issue/{issue_key}/comment"

    def __init__(self, client, jql: str) -> None:
        """Wrap a JiraClient with the JQL selecting issues in scope."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = client
        self.jql = jql

    def get_issues(self) -> list[dict]:
        """Fetch every issue matching the JQL, keeping only fields events need."""
        issues = []

        for raw in self.client.paginate_token(
            self.SEARCH_ENDPOINT,
            {
                "jql": self.jql,
                "maxResults": PAGE_SIZE,
                "fields": ",".join(SEARCH_FIELDS),
            },
            "issues",
        ):
            fields = raw.get("fields") or {}
            issues.append(
                {
                    "issue_key": raw.get("key"),
                    "project_key": (fields.get("project") or {}).get("key"),
                    "issue_type": (fields.get("issuetype") or {}).get("name"),
                    "created": self.client.to_bq_timestamp(fields.get("created")),
                    "reporter": fields.get("reporter") or {},
                }
            )

        self.logger.info("Found %s issues in scope", len(issues))
        return issues

    def _fetch_changelog(self, issue_key: str) -> Iterator[dict]:
        """Yield changelog histories for an issue, failing loudly on an unexpected page shape."""
        path = self.CHANGELOG_ENDPOINT.format(issue_key=issue_key)
        page = self.client.get(path, {"maxResults": PAGE_SIZE, "startAt": 0})

        if CHANGELOG_ITEMS_KEY not in page and page.get("total"):
            raise RuntimeError(
                f"Changelog response for {issue_key} has no {CHANGELOG_ITEMS_KEY!r} key but reports "
                f"total={page.get('total')}; keys were {sorted(page)}. The Jira changelog pagination "
                "shape has changed - update CHANGELOG_ITEMS_KEY."
            )

        yield from self.client.paginate_start_at(
            path, {"maxResults": PAGE_SIZE}, CHANGELOG_ITEMS_KEY
        )

    def iter_events(self, issues: list[dict]) -> Iterator[dict]:
        """Yield every event for the given issues, one issue at a time."""
        for index, issue in enumerate(issues, start=1):
            issue_key = issue["issue_key"]

            if index % 100 == 0:
                self.logger.info("Fetched events for %s/%s issues", index, len(issues))

            created = created_event(issue)
            if created:
                yield created

            for history in self._fetch_changelog(issue_key):
                yield from changelog_events(issue, history, self.client.to_bq_timestamp)

            for comment in self.client.paginate_start_at(
                self.COMMENT_ENDPOINT.format(issue_key=issue_key),
                {"maxResults": PAGE_SIZE},
                "comments",
            ):
                event = comment_event(issue, comment, self.client.to_bq_timestamp)
                if event:
                    yield event


def events_on_date(events: Iterable[dict], target_date: date) -> Iterator[dict]:
    """Yield only events whose UTC event date matches target_date."""
    prefix = target_date.isoformat()

    for event in events:
        if (event.get("event_ts") or "").startswith(prefix):
            yield event


class EventsBigQueryAPI:
    """Load Jira event rows into a partitioned BigQuery table."""

    def __init__(self) -> None:
        """Initialize a logger for load operations."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_events(
        self,
        destination: str,
        events: Iterable[dict],
        date_partition: Optional[str] = None,
        write_append: bool = False,
    ) -> int:
        """Stream events through a temporary NDJSON file into BigQuery.

        Events are written to disk as they are produced so that memory stays
        constant regardless of how many there are. Returns the row count.
        """
        job_config = bigquery.LoadJobConfig(
            schema=EVENT_SCHEMA,
            autodetect=False,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if write_append
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="event_ts"
            ),
        )

        table = f"{destination}${date_partition}" if date_partition else destination
        client = bigquery.Client()
        count = 0

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".ndjson") as tmp:
            for event in events:
                tmp.write(json.dumps(event).encode("utf-8"))
                tmp.write(b"\n")
                count += 1

            tmp.flush()
            tmp.seek(0)

            job = client.load_table_from_file(tmp, table, job_config=job_config)
            job.result()

        self.logger.info("Loaded %s events to %s", count, table)
        return count
