import os
from pathlib import Path

from generate_queries import TemplatedDir, get_query_dirs

BASE_DIR = Path(os.path.dirname(__file__)).parent


class TestGenerateQueries:
    def test_get_query_dirs(self):
        res = list(get_query_dirs(BASE_DIR / "templates"))
        assert res == [
            TemplatedDir("firefox_desktop_use_counters_v2", BASE_DIR / "templates" / "firefox_desktop_use_counters_v2")
        ]