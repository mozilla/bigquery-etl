from setuptools import find_namespace_packages, setup


def get_version():
    version = {}
    with open("bigquery_etl/_version.py") as fp:
        exec(fp.read(), version)

    return version["__version__"]


setup(
    name="mozilla-bigquery-etl",
    version=get_version(),
    author="Mozilla Corporation",
    author_email="fx-data-dev@mozilla.org",
    description="Tooling for building derived datasets in BigQuery",
    url="https://github.com/mozilla/bigquery-etl",
    packages=find_namespace_packages(include=["bigquery_etl.*", "bigquery_etl"]),
    package_data={
        "bigquery_etl": [
            "query_scheduling/templates/*.j2",
            "alchemer/*.json",
            "stripe/*.json",
            "stripe/*.yaml",
        ]
    },
    include_package_data=True,
    install_requires=[
        "gcloud",
        "gcsfs",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "Jinja2",
        "pytest-black",
        "pytest-pydocstyle",
        "pytest-flake8",
        "pytest-mypy",
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
        "pandas",
        "ujson",
        "stripe",
        "authlib",
    ],
    extras_require={"java": ["pyjnius"]},
    long_description="Tooling for building derived datasets in BigQuery",
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    entry_points="""
        [console_scripts]
        bqetl=bigquery_etl.cli:cli
    """,
)
