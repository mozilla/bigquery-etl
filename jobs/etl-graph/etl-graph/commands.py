import json
import logging
import statistics
from pathlib import Path

import click
import networkx as nx

from .config import *
from .crawler import fetch_dataset_listing, fetch_table_listing, resolve_view_references
from .utils import ensure_folder, ndjson_load, qualify, run, run_query

ROOT = Path(__file__).parent.parent


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--data-root", type=click.Path(file_okay=False), default=ROOT / "public" / "data"
)
def crawl(data_root):
    """Crawl bigquery projects."""
    run(f"gsutil ls gs://{BUCKET}")
    run(f"bq ls {PROJECT}:{DATASET}")

    data_root = ensure_folder(data_root)
    project = "moz-fx-data-shared-prod"
    dataset_listing = fetch_dataset_listing(project, data_root)
    tables_listing = fetch_table_listing(dataset_listing, data_root / project)

    views_listing = [row for row in tables_listing if row["table_type"] == "VIEW"]
    resolve_view_references(views_listing, data_root / project)


@cli.command()
@click.argument("query")
@click.option(
    "--data-root", type=click.Path(file_okay=False), default=ROOT / "public" / "data"
)
@click.option("--project", default="moz-fx-data-shared-prod")
def query_logs(query, data_root, project):
    """Get all queries made by service accounts in a project."""
    resources = Path(__file__).parent / "resources"
    sql = resources / f"{query}.sql"
    if not sql.exists():
        items = [p.name.strip(".sql") for p in resources.glob("sql")]
        raise ValueError(f"must be one of {items}")
    run_query(
        sql.read_text(),
        dest_table=query,
        output=ensure_folder(data_root) / project,
        project=project,
    )


@cli.command()
@click.option(
    "--data-root", type=click.Path(file_okay=False), default=ROOT / "public" / "data"
)
def index(data_root):
    """Combine all of the files together."""
    # currently, only combine view references and query_edgelist
    data_root = ensure_folder(data_root)
    edges = []
    nodes = []

    for edgelist in data_root.glob("**/query_log_edges.json"):
        rows = json.loads(edgelist.read_text())
        logging.info(
            f"merging {edgelist.relative_to(data_root)} with {len(rows)} query references"
        )
        edges += rows

    for nodelist in data_root.glob("**/query_log_nodes.json"):
        rows = json.loads(nodelist.read_text())
        logging.info(
            f"merging {nodelist.relative_to(data_root)} with {len(rows)} queries"
        )
        nodes += rows

    # write the file to disk as both csv and json, csv target is gephi compatible
    with (data_root / "edges.json").open("w") as fp:
        json.dump(edges, fp, indent=2)
    with (data_root / "nodes.json").open("w") as fp:
        json.dump(nodes, fp, indent=2)
    logging.info("wrote nodes.json")
    with (data_root / "edges.csv").open("w") as fp:
        fp.write("Source,Target\n")
        for edge in edges:
            fp.write(f"{edge['referenced_table']},{edge['destination_table']}\n")
    logging.info("wrote edges.csv")

    # generate some stats
    stats = {}
    G = nx.DiGraph()
    for edge in edges:
        G.add_edge(edge["referenced_table"], edge["destination_table"])
    stats["number_of_nodes"] = G.number_of_nodes()
    stats["number_of_edges"] = G.number_of_edges()
    stats["in_degree"] = sorted(dict(G.in_degree()).values())
    stats["out_degree"] = sorted(dict(G.out_degree()).values())
    stats["degree"] = sorted(dict(G.degree()).values())
    for value in ["in_degree", "out_degree", "degree"]:
        stats[f"avg_{value}"] = statistics.mean(stats[value])
    stats[
        "number_strongly_connected_components"
    ] = nx.number_strongly_connected_components(G)
    stats["number_weakly_connected_components"] = nx.number_weakly_connected_components(
        G
    )

    with (data_root / "network_stats.json").open("w") as fp:
        json.dump(stats, fp, indent=2)
    logging.info("wrote network_stats.json")

    # also generate a manifest so we can download the files via the app
    with (data_root / "manifest.json").open("w") as fp:
        json.dump(
            sorted(
                [
                    {
                        "path": str(p.relative_to(data_root.parent)),
                        "size_bytes": p.stat().st_size,
                    }
                    for p in data_root.glob("**/*")
                    if p.name != "manifest.json" and p.is_file()
                ],
                key=lambda x: x["path"],
            ),
            fp,
            indent=2,
        )
    logging.info("wrote manifest.json")
