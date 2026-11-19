"""Bind the deployed table artifacts to what `load_events` actually sends.

`EVENT_SCHEMA` and the table's `schema.yaml` are independent declarations of the
same thing, as are the partitioning/clustering in `metadata.yaml` and the
`LoadJobConfig` in `load_events`. Drift in either direction either breaks a load
job or silently rewrites the deployed table definition, so assert the agreement.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from bigquery_etl.jira.events import EVENT_SCHEMA, EventsBigQueryAPI

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLE_DIR = (
    REPO_ROOT
    / "sql"
    / "moz-fx-data-shared-prod"
    / "jira_events_derived"
    / "srein_events_v1"
)


def _load_yaml(name):
    with open(TABLE_DIR / name) as stream:
        return yaml.safe_load(stream)


def _job_config():
    """Return the LoadJobConfig load_events builds, without touching BigQuery."""
    captured = {}

    def fake_load(file_obj, table, job_config=None):
        captured["job_config"] = job_config
        return MagicMock()

    client = MagicMock()
    client.load_table_from_file.side_effect = fake_load

    with patch("bigquery_etl.jira.events.bigquery.Client", return_value=client):
        EventsBigQueryAPI().load_events(destination="p.d.t", events=iter([]))

    return captured["job_config"]


def test_schema_yaml_matches_event_schema():
    declared = [
        (field["name"], field["type"].upper(), field.get("mode", "NULLABLE").upper())
        for field in _load_yaml("schema.yaml")["fields"]
    ]
    coded = [
        (field.name, field.field_type.upper(), (field.mode or "NULLABLE").upper())
        for field in EVENT_SCHEMA
    ]

    assert declared == coded


def test_metadata_partitioning_matches_the_load_job_config():
    metadata = _load_yaml("metadata.yaml")["bigquery"]["time_partitioning"]
    time_partitioning = _job_config().time_partitioning

    assert metadata["field"] == time_partitioning.field
    assert metadata["type"].upper() == time_partitioning.type_


def test_metadata_clustering_matches_the_load_job_config():
    metadata = _load_yaml("metadata.yaml")["bigquery"]["clustering"]

    assert metadata["fields"] == _job_config().clustering_fields
