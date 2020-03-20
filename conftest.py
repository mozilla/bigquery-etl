"""PyTest configuration."""

import pytest


pytest_plugins = [
    "bigquery_etl.pytest_plugin.sql",
    "bigquery_etl.pytest_plugin.udf",
    "bigquery_etl.pytest_plugin.script_lint.black",
    "bigquery_etl.pytest_plugin.script_lint.docstyle",
    "bigquery_etl.pytest_plugin.script_lint.flake8",
    "bigquery_etl.pytest_plugin.script_lint.mypy",
]


def pytest_collection_modifyitems(config, items):
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return

    skip_publish_json_script = pytest.mark.skip(
        reason='publish_json_script marker not selected'
    )

    for item in items:
        if 'publish_json_script' in item.keywords:
            item.add_marker(skip_publish_json_script)
