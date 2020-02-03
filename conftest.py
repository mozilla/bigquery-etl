"""PyTest configuration."""

pytest_plugins = [
    "bigquery_etl.pytest_plugin.sql",
    "bigquery_etl.pytest_plugin.udf",
    "bigquery_etl.pytest_plugin.script_lint.black",
    "bigquery_etl.pytest_plugin.script_lint.docstyle",
    "bigquery_etl.pytest_plugin.script_lint.flake8",
    "bigquery_etl.pytest_plugin.script_lint.mypy",
]
