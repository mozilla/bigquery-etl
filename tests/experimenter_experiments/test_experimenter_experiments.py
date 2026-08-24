"""Tests for the metric-config sidecar read in experimenter_experiments_v1/query.py."""

import datetime
import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytz

QUERY_DIR = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "moz-fx-data-experiments"
    / "monitoring"
    / "experimenter_experiments_v1"
)
QUERY_PY = QUERY_DIR / "query.py"

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not QUERY_PY.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)


def load_query_module():
    # Load the module dynamically since the path contains hyphens.
    spec = importlib.util.spec_from_file_location(
        "experimenter_experiments_query", QUERY_PY
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def query():
    return load_query_module()


class TestReadMetricConfigs:
    def test_returns_configs_keyed_by_slug(self, query):
        fake_rows = [
            MagicMock(
                normandy_slug="slug-a", metric_config={"has_external_config": True}
            ),
            MagicMock(
                normandy_slug="slug-b", metric_config={"has_external_config": False}
            ),
        ]
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = fake_rows
            result = query.read_metric_configs(
                "proj", "monitoring", "experiment_metric_configs_v1"
            )

        assert result == {
            "slug-a": {"has_external_config": True},
            "slug-b": {"has_external_config": False},
        }

    def test_query_failure_returns_empty_dict(self, query):
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.side_effect = Exception("boom")
            result = query.read_metric_configs(
                "proj", "monitoring", "experiment_metric_configs_v1"
            )

        assert result == {}

    def test_date_time_results_are_json_serializable(self, query):
        fake_rows = [
            MagicMock(
                normandy_slug="slug-a",
                metric_config={
                    "external_config_last_modified": pytz.utc.localize(
                        datetime.datetime(2024, 1, 1, 12, 30)
                    ),
                    "overrides": {"start_date": datetime.date(2024, 6, 1)},
                },
            )
        ]
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = fake_rows
            result = query.read_metric_configs(
                "proj", "monitoring", "experiment_metric_configs_v1"
            )

        json.dumps(result)
        assert (
            result["slug-a"]["external_config_last_modified"]
            == "2024-01-01T12:30:00+00:00"
        )
        assert result["slug-a"]["overrides"]["start_date"] == "2024-06-01"
