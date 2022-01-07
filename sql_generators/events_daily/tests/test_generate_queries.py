import os
from pathlib import Path

from generate_queries import TemplatedDir, get_query_dirs

BASE_DIR = Path(os.path.dirname(__file__)).parent


class TestGenerateQueries:
    def test_get_query_dirs(self):
        res = list(get_query_dirs(BASE_DIR / "templates"))
        assert res == [
            TemplatedDir("event_types", BASE_DIR / "templates" / "event_types")
        ]
