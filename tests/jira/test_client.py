import pytest
import responses

from bigquery_etl.jira.client import MAX_ATTEMPTS, JiraClient

BASE = "https://jira.example.com"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.setenv("JIRA_TOKEN", "secret")
    return JiraClient(BASE)


def test_missing_username_raises(monkeypatch):
    monkeypatch.delenv("JIRA_USERNAME", raising=False)
    monkeypatch.setenv("JIRA_TOKEN", "secret")
    with pytest.raises(ValueError, match="JIRA_USERNAME"):
        JiraClient(BASE)


def test_missing_token_raises(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.delenv("JIRA_TOKEN", raising=False)
    with pytest.raises(ValueError, match="JIRA_TOKEN"):
        JiraClient(BASE)


def test_trailing_slash_stripped(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.setenv("JIRA_TOKEN", "secret")
    assert JiraClient(BASE + "/").base_jira_url == BASE


@responses.activate
def test_get_returns_json(client):
    responses.get(f"{BASE}/x", json={"ok": True}, status=200)
    assert client.get("/x", {}) == {"ok": True}


@responses.activate
def test_get_raises_on_404_without_retry(client):
    responses.get(f"{BASE}/x", json={}, status=404)
    with pytest.raises(RuntimeError, match="status_code=404"):
        client.get("/x", {})
    assert len(responses.calls) == 1


@responses.activate
def test_get_retries_429_honoring_retry_after(client, monkeypatch):
    slept = []
    monkeypatch.setattr("bigquery_etl.jira.client.time.sleep", slept.append)
    responses.get(f"{BASE}/x", json={}, status=429, headers={"Retry-After": "7"})
    responses.get(f"{BASE}/x", json={"ok": True}, status=200)

    assert client.get("/x", {}) == {"ok": True}
    assert slept == [7.0]


@responses.activate
def test_get_retries_503_with_exponential_backoff(client, monkeypatch):
    slept = []
    monkeypatch.setattr("bigquery_etl.jira.client.time.sleep", slept.append)
    responses.get(f"{BASE}/x", json={}, status=503)
    responses.get(f"{BASE}/x", json={}, status=503)
    responses.get(f"{BASE}/x", json={"ok": True}, status=200)

    assert client.get("/x", {}) == {"ok": True}
    assert slept == [2.0, 4.0]


@responses.activate
def test_get_raises_after_exhausting_retries(client, monkeypatch):
    monkeypatch.setattr("bigquery_etl.jira.client.time.sleep", lambda _: None)
    for _ in range(MAX_ATTEMPTS):
        responses.get(f"{BASE}/x", json={}, status=429)

    with pytest.raises(RuntimeError, match="status_code=429"):
        client.get("/x", {})
    assert len(responses.calls) == MAX_ATTEMPTS


@responses.activate
def test_paginate_start_at_walks_pages(client):
    responses.get(
        f"{BASE}/c",
        json={
            "startAt": 0,
            "maxResults": 2,
            "total": 3,
            "values": [{"id": 1}, {"id": 2}],
        },
    )
    responses.get(
        f"{BASE}/c",
        json={"startAt": 2, "maxResults": 2, "total": 3, "values": [{"id": 3}]},
    )

    assert list(client.paginate_start_at("/c", {}, "values")) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


@responses.activate
def test_paginate_start_at_stops_on_empty_page(client):
    responses.get(f"{BASE}/c", json={"total": 99, "values": []})
    assert list(client.paginate_start_at("/c", {}, "values")) == []


@responses.activate
def test_paginate_start_at_raises_when_total_is_absent(client):
    responses.get(f"{BASE}/c", json={"maxResults": 2, "values": [{"id": 1}, {"id": 2}]})

    with pytest.raises(RuntimeError, match=r"no 'total' field"):
        list(client.paginate_start_at("/c", {}, "values"))


@responses.activate
def test_paginate_start_at_names_the_path_when_total_is_absent(client):
    responses.get(f"{BASE}/c", json={"values": []})

    with pytest.raises(RuntimeError, match="/c"):
        list(client.paginate_start_at("/c", {}, "values"))


@responses.activate
def test_paginate_start_at_consumes_a_supplied_first_page(client):
    first_page = {
        "startAt": 0,
        "maxResults": 2,
        "total": 3,
        "values": [{"id": 1}, {"id": 2}],
    }
    responses.get(
        f"{BASE}/c",
        json={"startAt": 2, "maxResults": 2, "total": 3, "values": [{"id": 3}]},
    )

    assert list(
        client.paginate_start_at("/c", {}, "values", first_page=first_page)
    ) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert len(responses.calls) == 1, "the supplied page must not be re-fetched"
    assert "startAt=2" in responses.calls[0].request.url


@responses.activate
def test_paginate_start_at_with_a_complete_first_page_makes_no_call(client):
    first_page = {"startAt": 0, "maxResults": 2, "total": 1, "values": [{"id": 1}]}

    assert list(
        client.paginate_start_at("/c", {}, "values", first_page=first_page)
    ) == [{"id": 1}]
    assert len(responses.calls) == 0


@responses.activate
def test_paginate_token_walks_pages(client):
    responses.get(
        f"{BASE}/s", json={"issues": [{"key": "A-1"}], "nextPageToken": "tok"}
    )
    responses.get(f"{BASE}/s", json={"issues": [{"key": "A-2"}]})

    assert list(client.paginate_token("/s", {}, "issues")) == [
        {"key": "A-1"},
        {"key": "A-2"},
    ]
    assert "nextPageToken=tok" in responses.calls[1].request.url


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2026-07-27T09:40:36.912-0400", "2026-07-27T13:40:36+00:00"),
        ("2026-07-27T09:40:36-0400", "2026-07-27T13:40:36+00:00"),
        ("2026-07-27T13:40:36.000+0000", "2026-07-27T13:40:36+00:00"),
        (None, None),
        ("", None),
        ("not-a-timestamp", None),
    ],
)
def test_to_bq_timestamp(value, expected):
    assert JiraClient.to_bq_timestamp(value) == expected
