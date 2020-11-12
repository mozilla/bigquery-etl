import os
import pytest

from bigquery_etl.events_daily.generate_queries import QueryDir, Template
from pathlib import Path


BASE_DIR = Path(os.path.dirname(__file__)).parent


class TestQueryDir:
    @pytest.fixture
    def query_dir(self):
        return QueryDir("event_types", Path(BASE_DIR / "templates" / "event_types"))

    def test_get_datasets(self, query_dir):
        assert query_dir.get_datasets(query_dir.get_args()) == [
            "dataset-1",
            "dataset-2",
        ]

    def test_get_datasets_with_arg(self, query_dir):
        assert query_dir.get_datasets(query_dir.get_args(), "dataset-2") == [
            "dataset-2"
        ]

    def test_get_templates(self, query_dir):
        assert query_dir.get_templates() == [
            Template("query.sql", query_dir.get_environment())
        ]

    def test_get_args(self, query_dir):
        assert query_dir.get_args() == {
            "dataset-1": {"key": "val1"},
            "dataset-2": {"key": "val2"},
        }
