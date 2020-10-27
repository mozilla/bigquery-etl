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
import shutil
from multiprocessing import Pool

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

ROOT = Path(run("git rev-parse --show-toplevel"))
SQL_ROOT = ROOT / "sql" / "glam-fenix-dev" / "glam_etl"
TEST_ROOT = Path(__file__).parent


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


def _dryrun(p):
    return (p, dryrun(p.read_text()))


def calculate_dependencies(queries):
    with Pool(20) as p:
        dryrun_references = p.map(_dryrun, queries)
    res = []
    for query, refs in dryrun_references:
        for ref in refs:
            res.append(
                (
                    f"{ref.project}:{ref.dataset_id}.{ref.table_id}",
                    f"{query.parent.parent.parent.name}:{query.parent.parent.name}.{query.parent.name}",
                )
            )
    return sorted(set(res))


@click.group()
def bootstrap():
    pass


# TODO: move this to bqetl glam glean
@bootstrap.command()
@click.option("--output", default=TEST_ROOT / "dependencies.json")
def deps(output):
    path = Path(output)
    deps = calculate_dependencies(
        [p for p in SQL_ROOT.glob("**/*.sql") if "__clients_daily" not in p.name]
    )
    path.write_text(
        json.dumps([dict(zip(["from", "to"], dep)) for dep in deps], indent=2)
    )


@bootstrap.command()
@click.option(
    "--dependency-file",
    default=TEST_ROOT / "dependencies.json",
    type=click.Path(dir_okay=False, exists=True),
)
def skeleton(dependency_file):
    deps = json.loads(Path(dependency_file).read_text())
    for query in list_queries(SQL_ROOT):
        # only tests for org_mozilla_fenix_glam_nightly
        if "org_mozilla_fenix_glam_nightly" not in query.name:
            continue

        print(f"copying schemas for {query.name}")
        # views are also considered aggregates
        path = TEST_ROOT / query.name / "test_minimal"
        path.mkdir(parents=True, exist_ok=True)

        # now copy over the schemas
        query_deps = [x["from"] for x in deps if query.name in x["to"]]
        print(f"found dependencies: {json.dumps(query_deps, indent=2)}")
        for dep in query_deps:
            project = dep.split(":")[0]
            dataset = dep.split(":")[1].split(".")[0]
            table = dep.split(".")[1]
            for schema in (ROOT / "sql" / project / dataset).glob(f"{table}/schema.*"):
                print(f"copied dependency {schema}")
                shutil.copyfile(
                    schema,
                    path
                    / (
                        (f"{project}.{dataset}." if dataset != "glam_etl" else "")
                        + f"{schema.parent.name}.{schema.name}"
                    ),
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
