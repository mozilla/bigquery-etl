"""PyTest configuration."""


def pytest_configure():
    """Generate SQL files before running tests."""
    exec(open("script/generate_sql").read())
