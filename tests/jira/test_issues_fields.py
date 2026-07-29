import pytest
import responses
from google.cloud import bigquery

from bigquery_etl.jira.issues import (
    OUTPUT_SCHEMA,
    JiraAPI,
    JiraField,
    build_schema,
    extract_value,
)

BASE = "https://jira.example.com"


@pytest.fixture(autouse=True)
def creds(monkeypatch):
    monkeypatch.setenv("JIRA_USERNAME", "bot@example.com")
    monkeypatch.setenv("JIRA_TOKEN", "secret")


# --- extract_value: the shapes Jira actually returns -------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Scalars pass through.
        ("plain text", "plain text"),
        (7, 7),
        (True, True),
        (None, None),
        # Objects: Jira nests the human-readable value under a different key
        # depending on the field kind, so try them in order of specificity.
        ({"name": "P1", "id": "3"}, "P1"),  # priority, status, resolution
        ({"displayName": "Ada Lovelace", "accountId": "a1"}, "Ada Lovelace"),  # user
        ({"value": "Self-Serve", "id": "10"}, "Self-Serve"),  # select customfield
        ({"key": "SREIN", "name": "MozCloud Intake"}, "MozCloud Intake"),  # project
        # An object with none of the known keys yields None rather than a dict,
        # because a dict would fail the STRING column at load time.
        ({"id": "10", "self": "https://..."}, None),
        # Lists map elementwise, for REPEATED columns.
        (["SREIN", "form"], ["SREIN", "form"]),
        ([{"name": "a"}, {"name": "b"}], ["a", "b"]),
        ([], []),
    ],
)
def test_extract_value(raw, expected):
    assert extract_value(raw) == expected


def test_extract_value_prefers_name_over_value():
    """Some customfields carry both; `name` is the label Jira shows."""
    assert extract_value({"name": "Shown", "value": "Other"}) == "Shown"


# --- build_schema ------------------------------------------------------------


def test_build_schema_without_extras_is_the_base_schema():
    assert build_schema() == OUTPUT_SCHEMA
    assert build_schema(()) == OUTPUT_SCHEMA


def test_base_schema_ends_with_updated():
    """Appended, not inserted: a new column at the end is additive for consumers."""
    assert [f.name for f in OUTPUT_SCHEMA][-1] == "updated"
    assert OUTPUT_SCHEMA[-1].field_type == "TIMESTAMP"


def test_build_schema_appends_extras_in_declared_order():
    extras = [
        JiraField("priority", "priority"),
        JiraField("labels", "labels", mode="REPEATED"),
        JiraField("story_points", "customfield_10014", bq_type="FLOAT64"),
    ]
    schema = build_schema(extras)

    assert schema[: len(OUTPUT_SCHEMA)] == OUTPUT_SCHEMA
    assert [(f.name, f.field_type, f.mode) for f in schema[len(OUTPUT_SCHEMA) :]] == [
        ("priority", "STRING", "NULLABLE"),
        ("labels", "STRING", "REPEATED"),
        ("story_points", "FLOAT64", "NULLABLE"),
    ]


def test_build_schema_rejects_a_column_that_collides_with_the_base_schema():
    """Silently shadowing `status` would produce two columns of the same name."""
    with pytest.raises(ValueError, match="collides"):
        build_schema([JiraField("status", "customfield_1")])


def test_build_schema_rejects_duplicate_extra_columns():
    with pytest.raises(ValueError, match="duplicate"):
        build_schema([JiraField("p", "priority"), JiraField("p", "customfield_1")])


# --- JiraAPI ----------------------------------------------------------------


ISSUE = {
    "key": "ABC-1",
    "fields": {
        "project": {"key": "ABC"},
        "issuetype": {"name": "Epic"},
        "summary": "Do the thing",
        "status": {"name": "In Progress"},
        "created": "2026-07-27T09:40:36.912-0400",
        "updated": "2026-07-28T11:00:00.000-0400",
        "resolutiondate": None,
        "priority": {"name": "P1"},
        "labels": ["SREIN", "form"],
        "assignee": {"displayName": "Ada Lovelace"},
    },
}


@responses.activate
def test_updated_is_populated_and_utc_normalized():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (issue,) = JiraAPI(BASE, "project = ABC").get_issues()

    assert issue["updated"] == "2026-07-28T15:00:00+00:00"
    assert issue["created"] == "2026-07-27T13:40:36+00:00"


@responses.activate
def test_no_extras_yields_exactly_the_base_columns():
    """Guards the two pre-existing tables: their output gains `updated` and nothing else."""
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (issue,) = JiraAPI(BASE, "project = ABC").get_issues()

    assert set(issue) == {f.name for f in OUTPUT_SCHEMA}


@responses.activate
def test_updated_is_always_requested_from_jira():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    JiraAPI(BASE, "project = ABC").get_issues()

    assert "updated" in responses.calls[0].request.url


@responses.activate
def test_extra_fields_are_extracted_onto_their_columns():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})
    extras = [
        JiraField("priority", "priority"),
        JiraField("assignee", "assignee"),
        JiraField("labels", "labels", mode="REPEATED"),
    ]

    (issue,) = JiraAPI(BASE, "project = ABC", extra_fields=extras).get_issues()

    assert issue["priority"] == "P1"
    assert issue["assignee"] == "Ada Lovelace"
    assert issue["labels"] == ["SREIN", "form"]
    assert set(issue) == {f.name for f in OUTPUT_SCHEMA} | {
        "priority",
        "assignee",
        "labels",
    }


@responses.activate
def test_extra_fields_are_added_to_the_jira_request():
    """A field Jira was never asked for comes back absent, not null."""
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    JiraAPI(
        BASE, "project = ABC", extra_fields=[JiraField("sre", "customfield_10420")]
    ).get_issues()

    assert "customfield_10420" in responses.calls[0].request.url


@responses.activate
def test_a_missing_extra_field_is_null_not_absent():
    """Every row must carry every column, or the NDJSON load produces a ragged table."""
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (issue,) = JiraAPI(
        BASE, "project = ABC", extra_fields=[JiraField("nope", "customfield_99999")]
    ).get_issues()

    assert issue["nope"] is None


@responses.activate
def test_a_missing_repeated_extra_field_is_an_empty_list():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (issue,) = JiraAPI(
        BASE,
        "project = ABC",
        extra_fields=[JiraField("tags", "customfield_99999", mode="REPEATED")],
    ).get_issues()

    assert issue["tags"] == []


@responses.activate
def test_timestamp_typed_extra_fields_are_normalized():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (issue,) = JiraAPI(
        BASE,
        "project = ABC",
        extra_fields=[JiraField("dup_updated", "updated", bq_type="TIMESTAMP")],
    ).get_issues()

    assert issue["dup_updated"] == "2026-07-28T15:00:00+00:00"


def test_jira_field_rejects_an_unknown_mode():
    with pytest.raises(ValueError, match="mode"):
        JiraField("x", "y", mode="SOMETHING")


def test_jira_field_defaults():
    f = JiraField("priority", "priority")
    assert (f.column, f.jira_field, f.bq_type, f.mode) == (
        "priority",
        "priority",
        "STRING",
        "NULLABLE",
    )


def test_build_schema_returns_bigquery_schema_fields():
    schema = build_schema([JiraField("priority", "priority")])
    assert all(isinstance(f, bigquery.SchemaField) for f in schema)
