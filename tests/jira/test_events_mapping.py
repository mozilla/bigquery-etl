from bigquery_etl.jira.client import JiraClient
from bigquery_etl.jira.events import (
    EVENT_SCHEMA,
    EXCLUDED_CHANGELOG_FIELDS,
    changelog_events,
    comment_event,
    created_event,
)

TO_TS = JiraClient.to_bq_timestamp

ISSUE = {
    "issue_key": "SREIN-1548",
    "project_key": "SREIN",
    "issue_type": "Task",
    "created": "2025-05-14T17:19:19+00:00",
    "reporter": {"accountId": "acct-reporter", "displayName": "Ada Reporter"},
}


def test_every_event_has_exactly_the_schema_keys():
    expected = {f.name for f in EVENT_SCHEMA}
    events = [
        created_event(ISSUE),
        comment_event(
            ISSUE, {"id": "1", "created": "2026-07-27T09:00:00.000-0400"}, TO_TS
        ),
        *changelog_events(
            ISSUE,
            {
                "id": "63",
                "created": "2026-07-27T09:40:36.912-0400",
                "author": {"accountId": "a", "displayName": "B"},
                "items": [{"field": "priority", "fieldtype": "jira"}],
            },
            TO_TS,
        ),
    ]
    for event in events:
        assert set(event) == expected


def test_created_event():
    assert created_event(ISSUE) == {
        "issue_key": "SREIN-1548",
        "project_key": "SREIN",
        "issue_type": "Task",
        "event_id": "created",
        "event_type": "created",
        "event_ts": "2025-05-14T17:19:19+00:00",
        "actor_id": "acct-reporter",
        "actor_name": "Ada Reporter",
        "field": None,
        "field_type": None,
        "from_value": None,
        "to_value": None,
        "from_id": None,
        "to_id": None,
        "comment_is_public": None,
        "comment_body": None,
    }


def test_created_event_without_timestamp_is_dropped():
    assert created_event({**ISSUE, "created": None}) is None


def test_created_event_without_reporter_has_null_actor():
    event = created_event({**ISSUE, "reporter": {}})
    assert event["actor_id"] is None and event["actor_name"] is None


def test_status_item_becomes_status_change():
    history = {
        "id": "6310029",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {"accountId": "acct-1", "displayName": "Steve Jalim"},
        "items": [
            {
                "field": "status",
                "fieldtype": "jira",
                "from": "10042",
                "fromString": "Selected for Development",
                "to": "3",
                "toString": "In Progress",
            }
        ],
    }
    (event,) = list(changelog_events(ISSUE, history, TO_TS))
    assert event["event_type"] == "status_change"
    assert event["event_id"] == "6310029"
    assert event["event_ts"] == "2026-07-27T13:40:36+00:00"
    assert event["actor_id"] == "acct-1"
    assert event["actor_name"] == "Steve Jalim"
    assert event["field"] == "status"
    assert event["field_type"] == "jira"
    assert event["from_value"] == "Selected for Development"
    assert event["to_value"] == "In Progress"
    assert event["from_id"] == "10042"
    assert event["to_id"] == "3"
    assert event["comment_is_public"] is None


def test_non_status_item_becomes_field_change():
    history = {
        "id": "1",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {},
        "items": [{"field": "assignee", "fieldtype": "jira", "toString": "Ada"}],
    }
    (event,) = list(changelog_events(ISSUE, history, TO_TS))
    assert event["event_type"] == "field_change"
    assert event["field"] == "assignee"


def test_multiple_items_share_event_id_and_timestamp():
    history = {
        "id": "777",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {},
        "items": [
            {"field": "status", "fieldtype": "jira", "toString": "Done"},
            {"field": "assignee", "fieldtype": "jira", "toString": "Ada"},
        ],
    }
    events = list(changelog_events(ISSUE, history, TO_TS))
    assert len(events) == 2
    assert {e["event_id"] for e in events} == {"777"}
    assert {e["event_ts"] for e in events} == {"2026-07-27T13:40:36+00:00"}
    assert {e["event_type"] for e in events} == {"status_change", "field_change"}


def test_excluded_fields_produce_no_rows():
    assert EXCLUDED_CHANGELOG_FIELDS == frozenset({"Last Comment", "Rank"})
    history = {
        "id": "1",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {},
        "items": [
            {
                "field": "Last Comment",
                "fieldtype": "custom",
                "fromString": "secret body text",
            },
            {"field": "Rank", "fieldtype": "custom", "toString": "0|i0002n:"},
            {"field": "labels", "fieldtype": "jira", "toString": "urgent"},
        ],
    }
    events = list(changelog_events(ISSUE, history, TO_TS))
    assert [e["field"] for e in events] == ["labels"]


def test_history_without_timestamp_is_dropped():
    history = {"id": "1", "created": None, "author": {}, "items": [{"field": "status"}]}
    assert list(changelog_events(ISSUE, history, TO_TS)) == []


def test_comment_event():
    comment = {
        "id": "10453221",
        "created": "2026-07-27T09:00:00.000-0400",
        "author": {"accountId": "acct-2", "displayName": "Bob Commenter"},
    }
    event = comment_event(ISSUE, comment, TO_TS)
    assert event["event_type"] == "commented"
    assert event["event_id"] == "10453221"
    assert event["event_ts"] == "2026-07-27T13:00:00+00:00"
    assert event["actor_name"] == "Bob Commenter"
    assert event["field"] is None
    assert event["comment_is_public"] is True


def test_comment_with_visibility_restriction_is_not_public():
    comment = {
        "id": "1",
        "created": "2026-07-27T09:00:00.000-0400",
        "visibility": {"type": "role", "value": "Administrators"},
    }
    assert comment_event(ISSUE, comment, TO_TS)["comment_is_public"] is False


def test_comment_with_null_visibility_is_public():
    """A present-but-null `visibility` must not invert the flag for every row."""
    comment = {
        "id": "1",
        "created": "2026-07-27T09:00:00.000-0400",
        "visibility": None,
    }
    assert comment_event(ISSUE, comment, TO_TS)["comment_is_public"] is True


def test_comment_with_empty_visibility_object_is_public():
    comment = {"id": "1", "created": "2026-07-27T09:00:00.000-0400", "visibility": {}}
    assert comment_event(ISSUE, comment, TO_TS)["comment_is_public"] is True


def test_comment_jsd_public_flag_wins_over_null_visibility():
    comment = {
        "id": "1",
        "created": "2026-07-27T09:00:00.000-0400",
        "jsdPublic": False,
        "visibility": None,
    }
    assert comment_event(ISSUE, comment, TO_TS)["comment_is_public"] is False


def test_comment_jsd_public_flag_wins_when_present():
    comment = {"id": "1", "created": "2026-07-27T09:00:00.000-0400", "jsdPublic": False}
    assert comment_event(ISSUE, comment, TO_TS)["comment_is_public"] is False


def test_comment_without_timestamp_is_dropped():
    assert comment_event(ISSUE, {"id": "1", "created": None}, TO_TS) is None


def test_description_changes_keep_their_full_text():
    """Same content class and same ACL as comment bodies, so it is not redacted."""
    history = {
        "id": "1",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {},
        "items": [
            {
                "field": "description",
                "fieldtype": "jira",
                "fromString": "the entire old ticket body",
                "toString": "the entire new ticket body",
            }
        ],
    }
    (event,) = list(changelog_events(ISSUE, history, TO_TS))

    assert event["from_value"] == "the entire old ticket body"
    assert event["to_value"] == "the entire new ticket body"


def test_comment_event_carries_the_flattened_body():
    comment = {
        "id": "10453221",
        "created": "2026-07-27T09:00:00.000-0400",
        "author": {"accountId": "acct-2", "displayName": "Bob Commenter"},
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "Triage notes: p1. cc "},
                        {"type": "mention", "attrs": {"id": "x", "text": "@Ada"}},
                    ],
                }
            ],
        },
    }
    event = comment_event(ISSUE, comment, TO_TS)

    assert event["comment_body"] == "Triage notes: p1. cc @Ada"
    assert event["event_type"] == "commented"


def test_comment_without_a_body_gets_an_empty_string():
    comment = {"id": "1", "created": "2026-07-27T09:00:00.000-0400"}
    assert comment_event(ISSUE, comment, TO_TS)["comment_body"] == ""


def test_non_comment_events_have_no_body():
    history = {
        "id": "1",
        "created": "2026-07-27T09:40:36.912-0400",
        "author": {},
        "items": [{"field": "status", "fieldtype": "jira", "toString": "Done"}],
    }
    (event,) = list(changelog_events(ISSUE, history, TO_TS))

    assert event["comment_body"] is None
    assert created_event(ISSUE)["comment_body"] is None
