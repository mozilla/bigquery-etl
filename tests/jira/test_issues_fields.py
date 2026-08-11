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


# --- DATETIME / DATE typed extras -------------------------------------------
#
# The IIM incident timing fields are free-text in Jira and arrive as
# "2026-07-25 09:49": no `T`, no seconds, no timezone offset. to_bq_timestamp
# only parses ISO-with-offset, so routing them through TIMESTAMP would silently
# yield NULL for every row. They are normalized to a canonical DATETIME instead,
# and a value that will not parse becomes NULL rather than failing the load job
# for the whole table.


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-07-25 09:49", "2026-07-25 09:49:00"),
        ("2026-07-25 09:49:12", "2026-07-25 09:49:12"),
        ("2026-07-25T09:49", "2026-07-25 09:49:00"),
        ("2026-07-25T09:49:12", "2026-07-25 09:49:12"),
        ("  2026-07-25 09:49  ", "2026-07-25 09:49:00"),
        # Free-text field, so anything can turn up. None beats a failed load.
        ("not a datetime", None),
        ("2026-13-45 99:99", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_datetime(raw, expected):
    from bigquery_etl.jira.issues import parse_datetime

    assert parse_datetime(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-07-25", "2026-07-25"),
        ("  2026-07-25 ", "2026-07-25"),
        # A datetime in a date field keeps only the date part.
        ("2026-07-25 09:49", "2026-07-25"),
        ("nope", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_date(raw, expected):
    from bigquery_etl.jira.issues import parse_date

    assert parse_date(raw) == expected


@responses.activate
def test_datetime_typed_extra_field_is_normalized():
    """Captured from IIM-308: customfield_18692 holds '2026-07-25 09:49'."""
    issue = {
        "key": "IIM-308",
        "fields": {**ISSUE["fields"], "customfield_18692": "2026-07-25 09:49"},
    }
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = IIM",
        extra_fields=[
            JiraField("time_declared", "customfield_18692", bq_type="DATETIME")
        ],
    ).get_issues()

    assert out["time_declared"] == "2026-07-25 09:49:00"


@responses.activate
def test_datetime_typed_extra_field_would_be_null_as_timestamp():
    """Pins the reason DATETIME exists: TIMESTAMP silently drops this format."""
    issue = {
        "key": "IIM-308",
        "fields": {**ISSUE["fields"], "customfield_18692": "2026-07-25 09:49"},
    }
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = IIM",
        extra_fields=[JiraField("as_ts", "customfield_18692", bq_type="TIMESTAMP")],
    ).get_issues()

    assert out["as_ts"] is None


@responses.activate
def test_date_typed_extra_field_is_normalized():
    issue = {
        "key": "IIM-308",
        "fields": {**ISSUE["fields"], "customfield_15087": "2026-07-25"},
    }
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = IIM",
        extra_fields=[JiraField("declare_date", "customfield_15087", bq_type="DATE")],
    ).get_issues()

    assert out["declare_date"] == "2026-07-25"


@responses.activate
def test_option_valued_extra_fields_use_their_value_key():
    """Severity and Detection Method are select fields: {'value': 'S3', 'id': ...}."""
    issue = {
        "key": "IIM-308",
        "fields": {
            **ISSUE["fields"],
            "customfield_10319": {"value": "S3", "id": "10866"},
            "customfield_12881": {"value": "Manual", "id": "14790"},
            "customfield_18555": "phabricator",
        },
    }
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = IIM",
        extra_fields=[
            JiraField("severity", "customfield_10319"),
            JiraField("detection_method", "customfield_12881"),
            JiraField("affected_entities", "customfield_18555"),
        ],
    ).get_issues()

    assert out["severity"] == "S3"
    assert out["detection_method"] == "Manual"
    assert out["affected_entities"] == "phabricator"


# --- review feedback: load-killing edge cases -------------------------------


def test_extract_value_drops_unusable_list_elements():
    """BigQuery rejects a null element inside a REPEATED column.

    The scalar branch returns None for an unrecognised object shape so a dict
    never reaches a STRING column. The list branch has to go further and drop
    those elements: a REPEATED column with [None] fails the load with "Array
    specifies a null element", which is the same outcome the scalar rule exists
    to prevent.
    """
    assert extract_value([{"id": "1", "self": "https://..."}]) == []
    assert extract_value([{"name": "a"}, {"id": "2"}, {"name": "b"}]) == ["a", "b"]
    assert extract_value([None, "keep", None]) == ["keep"]


@responses.activate
def test_timestamp_typed_extra_field_survives_an_object_value():
    """A TIMESTAMP field reconfigured to a select must not kill the run.

    to_bq_timestamp hands its argument to strptime, which raises TypeError on a
    dict. Only ValueError is caught, so the exception would escape and take the
    whole load down rather than nulling one cell.
    """
    issue = {
        "key": "ABC-1",
        "fields": {**ISSUE["fields"], "customfield_1": {"value": "not a date"}},
    }
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = ABC",
        extra_fields=[JiraField("ts", "customfield_1", bq_type="TIMESTAMP")],
    ).get_issues()

    assert out["ts"] is None


@responses.activate
def test_repeated_field_wraps_a_bare_scalar():
    """A single-value multi-select comes back unwrapped; keep the value."""
    issue = {"key": "ABC-1", "fields": {**ISSUE["fields"], "customfield_1": "solo"}}
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [issue]})

    (out,) = JiraAPI(
        BASE,
        "project = ABC",
        extra_fields=[JiraField("tags", "customfield_1", mode="REPEATED")],
    ).get_issues()

    assert out["tags"] == ["solo"]


@responses.activate
def test_repeated_field_still_empty_when_jira_omits_it():
    responses.get(f"{BASE}/rest/api/3/search/jql", json={"issues": [ISSUE]})

    (out,) = JiraAPI(
        BASE,
        "project = ABC",
        extra_fields=[JiraField("tags", "customfield_99999", mode="REPEATED")],
    ).get_issues()

    assert out["tags"] == []


@pytest.mark.parametrize("bad", ["DATETIEM", "TIMESTAMPZ", "", "string"])
def test_jira_field_rejects_an_unknown_bq_type(bad):
    """Caught at declaration, not at load: _extra_values would silently fall through."""
    with pytest.raises(ValueError, match="bq_type"):
        JiraField("x", "customfield_1", bq_type=bad)


@pytest.mark.parametrize("good", ["STRING", "TIMESTAMP", "DATETIME", "DATE", "INT64"])
def test_jira_field_accepts_supported_bq_types(good):
    assert JiraField("x", "customfield_1", bq_type=good).bq_type == good


def test_repeated_mode_rejects_a_parsed_scalar_type():
    """DATE/DATETIME/TIMESTAMP parsers return a scalar, so REPEATED is always [].

    Nothing declares this combination today; rejecting it stops a future table
    from getting a permanently empty column with no error.
    """
    for bq_type in ("DATE", "DATETIME", "TIMESTAMP"):
        with pytest.raises(ValueError, match="REPEATED"):
            JiraField("x", "customfield_1", bq_type=bq_type, mode="REPEATED")
