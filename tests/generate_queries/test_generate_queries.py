import os

from bigquery_etl.events_daily.generate_queries import get_query_dirs, TemplatedDir
from pathlib import Path

BASE_DIR = Path(os.path.dirname(__file__)).parent


class TestGenerateQueries:
    def test_get_query_dirs(self):
        res = list(get_query_dirs(BASE_DIR / "templates"))
        assert res == [
            TemplatedDir("event_types", BASE_DIR / "templates" / "event_types")
        ]
