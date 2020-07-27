from setuptools import find_packages, setup

setup(
    name="mozilla-bigquery-etl",
    use_incremental=True,
    author="Mozilla Corporation",
    author_email="fx-data-dev@mozilla.org",
    description="Tooling for building derived datasets in BigQuery",
    url="https://github.com/mozilla/bigquery-etl",
    packages=["bigquery_etl", "bigquery_etl.cli", "bigquery_etl.metadata"],
    package_data={},
    install_requires=[
        "gcloud",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "Jinja2",
        "pytest-black",
        "pytest-docstyle",
        "pytest-flake8",
        "pytest-mypy",
        "pytest-xdist",
        "pytest-dependency",
        "pytest",
        "PyYAML",
        "smart_open",
        "sqlparse",
        "mozilla_schema_generator",
        "GitPython",
        "cattrs",
        "attrs",
        "typing",
        "click",
    ],
    long_description="Tooling for building derived datasets in BigQuery",
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    entry_points="""
        [console_scripts]
        bqetl=bigquery_etl.cli:cli
    """,
)