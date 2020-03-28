"""Generate templated views."""
from pathlib import Path
from argparse import ArgumentParser, Namespace
from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat

from enum import Enum
from functools import partial


class QueryType:
    VIEW = "view"
    INIT = "init"
    TABLE = "query"


def from_template(
    query_type: QueryType,
    template_name: str,
    args: Namespace,
    dataset_path: Path,
    **kwargs,
) -> str:
    # load template and get output view name
    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))
    template = env.get_template(f"{template_name}.sql")
    table_id = f"{args.prefix}_{template_name}"

    # create the directory for the view
    (dataset_path / table_id).mkdir(exist_ok=True)
    view_path = dataset_path / table_id / f"{query_type}.sql"

    # write the query with appropriate variables
    query_text = reformat(template.render(**{**vars(args), **kwargs}))

    print(f"writing {view_path}")
    with view_path.open("w") as fp:
        print(query_text, file=fp)

    return query_text


def main():
    """Generate GLAM ETL queries."""
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument("--prefix", default="fenix")
    parser.add_argument("--dataset", default="glam_etl")
    parser.add_argument("--sql-root", default="sql/")
    args = parser.parse_args()

    dataset_path = Path(args.sql_root) / args.dataset
    if not dataset_path.is_dir():
        raise NotADirectoryError(f"path to {dataset_path} not found")

    # curry functions for convenience
    template = partial(from_template, dataset_path=dataset_path, args=args)
    view = partial(template, QueryType.VIEW)
    table = partial(template, QueryType.TABLE)

    table(
        "latest_versions_v1",
        **dict(source_table="org_mozilla_fenix_stable.baseline_v1"),
    )
    view("view_clients_daily_scalar_aggregates_v1")
    view("view_clients_daily_histogram_aggregates_v1")
    view("view_client_probe_counts_v1")


if __name__ == "__main__":
    main()
