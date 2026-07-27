import pytest
import responses

from bigquery_etl.jira.client import JiraClient
from bigquery_etl.jira.events import JiraEventsAPI

BASE = "https://jira.example.com"

SEARCH_RESPONSE = {
    "issues": [
        {
            "key": "SREIN-1",
            "fields": {
                "project": {"key": "SREIN"},
                "issuetype": {"name": "Task"},
                "created": "2025-05-14T17:19:19.438-0400",
                "reporter": {"accountId": "acct-r", "displayName": "Ada Reporter"},
            },
        }
    ]
}

CHANGELOG_RESPONSE = {
    "startAt": 0,
    "maxResults": 100,
    "total": 1,
    "values": [
        {
            "id": "63",
            "created": "2026-07-27T09:40:36.912-0400",
            "author": {"accountId": "acct-1", "displayName": "Steve Jalim"},
            "items": [
                {"field": "status", "fieldtype": "jira", "toString": "In Progress"},
                {"field": "Rank", "fieldtype": "custom", "toString": "0|i:"},
            ],
        }
    ],
}

COMMENT_RESPONSE = {
    "startAt": 0,
    "maxResults": 100,
    "total": 1,
    "comments": [
        {
            "id": "999",
            "created": "2026-07-27T10:00:00.000-0400",
            "author": {"accountId": "acct-2", "displayName": "Bob Commenter"},
            "body": {"type": "doc", "content": [{"type": "paragraph"}]},
        }
    ],
}


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.setenv("JIRA_TOKEN", "secret")
    return JiraEventsAPI(JiraClient(BASE), "project = SREIN")


@responses.activate
def test_get_issues_maps_and_requests_only_needed_fields(api):
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_RESPONSE)

    assert api.get_issues() == [
        {
            "issue_key": "SREIN-1",
            "project_key": "SREIN",
            "issue_type": "Task",
            "created": "2025-05-14T21:19:19+00:00",
            "reporter": {"accountId": "acct-r", "displayName": "Ada Reporter"},
        }
    ]
    url = responses.calls[0].request.url
    assert "jql=project+%3D+SREIN" in url
    assert "issuetype" in url and "reporter" in url
    assert "summary" not in url


@responses.activate
def test_iter_events_combines_created_changelog_and_comments(api):
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_RESPONSE)
    responses.get(f"{BASE}/rest/api/3/issue/SREIN-1/changelog", json=CHANGELOG_RESPONSE)
    responses.get(f"{BASE}/rest/api/3/issue/SREIN-1/comment", json=COMMENT_RESPONSE)

    events = list(api.iter_events(api.get_issues()))

    assert [e["event_type"] for e in events] == [
        "created",
        "status_change",
        "commented",
    ]
    assert all(e["issue_key"] == "SREIN-1" for e in events)
    assert all(e["project_key"] == "SREIN" for e in events)
    # "Rank" was dropped, so the changelog contributed exactly one row.
    assert sum(1 for e in events if e["event_id"] == "63") == 1


@responses.activate
def test_iter_events_is_lazy(api):
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_RESPONSE)
    responses.get(f"{BASE}/rest/api/3/issue/SREIN-1/changelog", json=CHANGELOG_RESPONSE)
    responses.get(f"{BASE}/rest/api/3/issue/SREIN-1/comment", json=COMMENT_RESPONSE)

    issues = api.get_issues()
    calls_after_search = len(responses.calls)
    generator = api.iter_events(issues)

    assert len(responses.calls) == calls_after_search, "no fetch before first next()"
    next(generator)
    assert len(responses.calls) == calls_after_search, "created event needs no fetch"


@responses.activate
def test_iter_events_propagates_a_failed_issue(api):
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_RESPONSE)
    responses.get(f"{BASE}/rest/api/3/issue/SREIN-1/changelog", json={}, status=404)

    with pytest.raises(RuntimeError, match="status_code=404"):
        list(api.iter_events(api.get_issues()))


@responses.activate
def test_unexpected_changelog_shape_raises(api):
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_RESPONSE)
    responses.get(
        f"{BASE}/rest/api/3/issue/SREIN-1/changelog",
        json={"startAt": 0, "maxResults": 100, "total": 27, "histories": [{"id": "1"}]},
    )

    with pytest.raises(RuntimeError, match="CHANGELOG_ITEMS_KEY"):
        list(api.iter_events(api.get_issues()))
