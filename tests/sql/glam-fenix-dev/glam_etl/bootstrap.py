#!/usr/bin/env python3
"""
Script for initializing tests for queries related to glam-fenix-dev.

This only needs to be run once for the initial testing.
"""
import json
import shutil
import warnings
from collections import namedtuple
from glob import glob
from multiprocessing import Pool
from pathlib import Path

import click
import yaml
from google.cloud import bigquery

from bigquery_etl.glam.utils import run

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

ROOT = Path(run("git rev-parse --show-toplevel"))
SQL_ROOT = ROOT / "sql" / "glam-fenix-dev" / "glam_etl"
TEST_ROOT = Path(__file__).parent


def list_queries(path):
    """List all of the queries in the glam_etl folder."""
    return list([p for p in path.glob("*") if "__clients_daily" not in p.name])


def dryrun(sql):
    """Dry run a query and return the referenced tables.

    This does not capture views, but only the tables referenced by those
    views.
    """
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


def _dryrun(query_path):
    """Return the dry run results from a path."""
    return (query_path, dryrun(query_path.read_text()))


def calculate_dependencies(queries):
    """Create a list of dependencies between queries for bootstrapping schemas."""
    with Pool(20) as p:
        dryrun_references = p.map(_dryrun, queries)
    res = []
    for query, refs in dryrun_references:
        for ref in refs:
            res.append(
                (
                    f"{ref.project}:{ref.dataset_id}.{ref.table_id}",
                    f"{query.parent.parent.parent.name}:"
                    f"{query.parent.parent.name}.{query.parent.name}",
                )
            )
    return sorted(set(res))


@click.group()
def bootstrap():
    """Script for initializing tests for queries related to glam-fenix-dev."""
    pass


@bootstrap.command()
@click.option("--output", default=TEST_ROOT / "dependencies.json")
def deps(output):
    """Create a dependency file with all links between queries and tables."""
    path = Path(output)
    deps = calculate_dependencies(
        [
            p
            for p in map(Path, glob(f"{SQL_ROOT}/**/*.sql", recursive=True))
            if "__clients_daily" not in p.name
        ]
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
def mermaid(dependency_file):
    """Generate a mermaid diagram of dependencies."""
    deps = json.loads(Path(dependency_file).read_text())

    # project, dataset, product, query
    Node = namedtuple("node", ["qualified", "project", "dataset", "name", "product"])

    def parse(x):
        project, dataset_table = x.split(":")
        dataset, table = dataset_table.split(".")
        is_glam = dataset == "glam_etl"
        return Node(
            dataset_table if is_glam else x,
            project,
            dataset,
            table.split("__")[1] if is_glam else table,
            table.split("__")[0] if is_glam else None,
        )

    # build up mapping of nodes to parsed info
    nodes = {}
    for dep in deps:
        src = dep["from"]
        dst = dep["to"]
        for x in [src, dst]:
            if "*" not in x:
                nodes[x] = parse(x)

    print("graph LR")
    projects = set(n.project for n in nodes.values())
    for project in projects:
        print(f'subgraph "{project}"')
        datasets = set(n.dataset for n in nodes.values() if n.project == project)
        for dataset in datasets:
            print(f'subgraph "{dataset}"')
            if dataset == "glam_etl":
                products = set(
                    n.product for n in nodes.values() if n.dataset == "glam_etl"
                )
                for product in products:
                    print(f'subgraph "{product}"')
                    for n in set(
                        n
                        for n in nodes.values()
                        if n.dataset == "glam_etl" and n.product == product
                    ):
                        print(f"{n.qualified}[{n.name}]")
                    print("end")
            else:
                for n in set(n for n in nodes.values() if n.dataset == dataset):
                    print(f"{n.qualified}[{n.name}]")
            print("end")
        print("end")

    for dep in deps:
        src = dep["from"]
        dst = dep["to"]
        if "*" in src:
            for node in [
                v for k, v in nodes.items() if k.startswith(src.replace("*", ""))
            ]:
                print(f"{node.qualified} --> {nodes[dst].qualified}")
        else:
            print(f"{nodes[src].qualified} --> {nodes[dst].qualified}")


@bootstrap.command()
@click.option(
    "--dependency-file",
    default=TEST_ROOT / "dependencies.json",
    type=click.Path(dir_okay=False, exists=True),
)
def skeleton(dependency_file):
    """Generate the skeleton for minimal tests.

    Schemas are populated from the sql folder. Run the `deps` command to
    generate a file of dependencies between queries.
    """
    deps = json.loads(Path(dependency_file).read_text())
    for query in list_queries(SQL_ROOT):
        # only tests for org_mozilla_fenix_glam_nightly
        if "org_mozilla_fenix_glam_nightly" not in query.name:
            continue

        print(f"copying schemas for {query.name}")
        # views are also considered aggregates
        path = TEST_ROOT / query.name / "test_minimal"
        path.mkdir(parents=True, exist_ok=True)

        # set query parameters
        params = list(
            map(
                lambda x: dict(zip(["name", "type", "value"], x)),
                [
                    ("submission_date", "DATE", "2020-10-01"),
                    ("min_sample_id", "INT64", 0),
                    ("max_sample_id", "INT64", 99),
                    ("sample_size", "INT64", 100),
                ],
            )
        )
        with (path / "query_params.yaml").open("w") as fp:
            yaml.dump(params, fp)

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
                        (
                            f"{project}.{dataset}."
                            if dataset != "glam_etl"
                            else f"{dataset}."
                        )
                        + f"{schema.parent.name}.{schema.name}"
                    ),
                )


if __name__ == "__main__":
    bootstrap()
