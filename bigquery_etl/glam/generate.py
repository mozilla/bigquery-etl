"""Generate templated views."""
from pathlib import Path
from argparse import ArgumentParser, Namespace
from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.glam import models

from dataclasses import dataclass
from functools import partial


class QueryType:
    """Types of queries in the template folder."""

    VIEW = "view"
    INIT = "init"
    TABLE = "query"


@dataclass
class TemplateResult:
    """Results of templating a query."""

    table_id: str
    query_type: QueryType
    query_text: str


def from_template(
    query_type: QueryType,
    template_name: str,
    environment: Environment,
    args: Namespace,
    dataset_path: Path,
    query_name_prefix=None,
    **kwargs,
) -> TemplateResult:
    """Fill in templates and write them to disk."""
    if query_type == QueryType.INIT:
        template = environment.get_template(f"{template_name}.init.sql")
    else:
        template = environment.get_template(f"{template_name}.sql")

    if query_name_prefix:
        table_id = f"{args.prefix}_{query_name_prefix}_{template_name}"
    else:
        table_id = f"{args.prefix}_{template_name}"

    # create the directory for the view
    (dataset_path / table_id).mkdir(exist_ok=True)
    view_path = dataset_path / table_id / f"{query_type}.sql"

    # write the query with appropriate variables
    query_text = reformat(template.render(**{**vars(args), **kwargs}))

    print(f"generated {view_path}")
    with view_path.open("w") as fp:
        print(query_text, file=fp)

    return TemplateResult(table_id, query_type, query_text)


def main():
    """Generate GLAM ETL queries."""
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument("--prefix", default="fenix")
    parser.add_argument("--dataset", default="glam_etl")
    parser.add_argument("--sql-root", default="sql/")
    args = parser.parse_args()

    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))

    dataset_path = Path(args.sql_root) / args.dataset
    if not dataset_path.is_dir():
        raise NotADirectoryError(f"path to {dataset_path} not found")

    # curry functions for convenience
    template = partial(
        from_template, environment=env, dataset_path=dataset_path, args=args
    )
    view = partial(template, QueryType.VIEW)
    table = partial(template, QueryType.TABLE)
    init = partial(template, QueryType.INIT)

    [
        table(
            "latest_versions_v1",
            **dict(source_table="org_mozilla_fenix_stable.baseline_v1"),
        ),
        init(
            "clients_scalar_aggregates_v1",
            **models.clients_scalar_aggregates(
                source_table=(
                    f"glam_etl.{args.prefix}_view_clients_daily_scalar_aggregates_v1"
                ),
                destination_table=(
                    f"glam_etl.{args.prefix}_clients_scalar_aggregates_v1"
                ),
            ),
        ),
        table(
            "clients_scalar_aggregates_v1",
            **models.clients_scalar_aggregates(
                source_table=(
                    f"glam_etl.{args.prefix}_view_clients_daily_scalar_aggregates_v1"
                ),
                destination_table=(
                    f"glam_etl.{args.prefix}_clients_scalar_aggregates_v1"
                ),
            ),
        ),
        init(
            "clients_histogram_aggregates_v1",
            **models.clients_histogram_aggregates(parameterize=True),
        ),
        table(
            "clients_histogram_aggregates_v1",
            **models.clients_histogram_aggregates(parameterize=True),
        ),
        table(
            "clients_scalar_bucket_counts_v1",
            **models.clients_scalar_bucket_counts(
                source_table="glam_etl.fenix_clients_scalar_aggregates_v1"
            ),
        ),
        table(
            "clients_histogram_bucket_counts_v1",
            **models.clients_histogram_bucket_counts(),
        ),
        table(
            "probe_counts_v1",
            query_name_prefix="clients_scalar",
            **models.probe_counts(
                source_table="glam_etl.fenix_clients_scalar_bucket_counts_v1",
                is_scalar=True,
            ),
        ),
        table(
            "probe_counts_v1",
            query_name_prefix="clients_histogram",
            **models.probe_counts(
                source_table="glam_etl.fenix_clients_histogram_bucket_counts_v1",
                is_scalar=False,
            ),
        ),
        table(
            "scalar_percentiles_v1",
            **models.scalar_percentiles(
                source_table="glam_etl.fenix_clients_scalar_aggregates_v1"
            ),
        ),
        view("view_clients_daily_scalar_aggregates_v1"),
        view("view_clients_daily_histogram_aggregates_v1"),
        table("histogram_percentiles_v1"),
        view("view_client_probe_counts_v1"),
    ]


if __name__ == "__main__":
    main()
