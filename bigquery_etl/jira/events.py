"""Fetch Jira ticket change events and load them into BigQuery."""

import logging
from typing import Callable, Iterator, Optional

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
