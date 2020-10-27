#!/usr/bin/env python3
# Script for initializing all of the tests for queries related to
# fenix-glam-dev. This only needs to be run once for the initial testing.

from pathlib import Path
from bigquery_etl.glam.utils import run, get_schema
import json
import yaml
from pprint import PrettyPrinter
from google.cloud import bigquery
import click
import warnings

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

ROOT = Path(run("git rev-parse --show-toplevel"))
SQL_ROOT = ROOT / "sql" / "glam-fenix-dev" / "glam_etl"


def list_queries(path):
    return list([p for p in path.glob("*") if "__clients_daily" not in p.name])


def dryrun(sql):
    client = bigquery.Client(project="glam-fenix-dev")
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("submission_date", "DATE", "2020-10-01"),
                bigquery.ScalarQueryParameter("min_sample_id", "INT64", 0),
                bigquery.ScalarQueryParameter("max_sample_id", "INT64", 99),
                bigquery.ScalarQueryParameter("sample_size", "INT64", 100),
            ],
        ),
    )
    return job.referenced_tables


def calculate_dependencies(queries):
    res = []
    for query in queries:
        for ref in dryrun(query.read_text()):
            res.append((ref.table_id, query.parent.name))
    return sorted(set(res))


@click.group()
def bootstrap():
    pass


# TODO: move this to bqetl glam glean
@bootstrap.command()
@click.option("--output", default=(Path(__file__).parent / "dependencies.json"))
def deps(output):
    path = Path(output)
    deps = calculate_dependencies(
        [p for p in SQL_ROOT.glob("**/*.sql") if "__clients_daily" not in p.name]
    )
    path.write_text(
        json.dumps([dict(zip(["from", "to"], dep)) for dep in deps], indent=2)
    )


@bootstrap.command()
def main():
    pp = PrettyPrinter()
    assert SQL_ROOT.exists(), f"{SQL_ROOT} does not exist"
    queries = list_queries(SQL_ROOT)
    query_names = sorted([q.name for q in queries])
    pp.pprint(query_names)

    # create folders if they don't exist


if __name__ == "__main__":
    bootstrap()
