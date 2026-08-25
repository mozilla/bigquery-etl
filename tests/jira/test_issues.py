import pytest
import responses

from bigquery_etl.jira.issues import JiraAPI

BASE = "https://jira.example.com"

SEARCH_PAGE = {
    "issues": [
        {
            "key": "ABC-1",
            "fields": {
                "project": {"key": "ABC"},
                "issuetype": {"name": "Epic"},
                "summary": "Do the thing",
                "status": {"name": "In Progress"},
                "created": "2026-07-27T09:40:36.912-0400",
                "updated": "2026-07-28T11:00:00.000-0400",
                "resolutiondate": None,
            },
        }
    ]
}


@pytest.fixture(autouse=True)
def creds(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.setenv("JIRA_TOKEN", "secret")


@responses.activate
def test_get_issues_maps_to_output_schema():
    responses.get(f"{BASE}/rest/api/3/search/jql", json=SEARCH_PAGE)

    assert JiraAPI(BASE, "project = ABC").get_issues() == [
        {
            "issue_key": "ABC-1",
            "project_key": "ABC",
            "issue_type": "Epic",
            "summary": "Do the thing",
            "status": "In Progress",
            "created": "2026-07-27T13:40:36+00:00",
            "resolved": None,
            "updated": "2026-07-28T15:00:00+00:00",
        }
    ]


@responses.activate
def test_get_issues_follows_pagination():
    responses.get(
        f"{BASE}/rest/api/3/search/jql",
        json={**SEARCH_PAGE, "nextPageToken": "tok"},
    )
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": []})

    assert len(JiraAPI(BASE, "project = ABC").get_issues()) == 1
    assert "nextPageToken=tok" in responses.calls[1].request.url
