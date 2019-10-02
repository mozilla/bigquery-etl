"""PyTest configuration."""
from bigquery_etl import sql_test_plugin, udf_test_plugin


def pytest_configure():
    """Generate SQL files before running tests."""
    with open("script/generate_sql") as fp:
        exec(fp.read())


def pytest_collect_file(parent, path):
    """Enable bigquery pytest plugins to collect non-python tests."""
    return (
        sql_test_plugin.pytest_collect_file(parent, path)
        or udf_test_plugin.pytest_collect_file(parent, path)
    )
