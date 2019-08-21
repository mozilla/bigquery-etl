"""PyTest configuration."""
from .util import SqlTest

expect_names = {f"expect.{ext}" for ext in ("yaml", "json", "ndjson")}


def pytest_configure():
    """Generate SQL files before running tests."""
    exec(open("script/generate_sql").read())


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if path.basename in expect_names:
        return SqlTest(path.parts()[-2], parent)
