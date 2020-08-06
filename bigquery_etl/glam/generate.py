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
        table_id = f"{args.prefix}__{query_name_prefix}_{template_name}"
    else:
        table_id = f"{args.prefix}__{template_name}"

    # replaces the header, if it exists
    kwargs["header"] = f"-- {query_type} for {table_id};"

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
    parser.add_argument("--prefix")
    parser.add_argument("--dataset", default="glam_etl")
    parser.add_argument("--sql-root", default="sql/")
    args = parser.parse_args()

    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))

    dataset_path = Path(args.sql_root) / args.dataset
    if not dataset_path.is_dir():
        raise NotADirectoryError(f"path to {dataset_path} not found")

    build_date_udf_mapping = dict(
        org_mozilla_fenix="`moz-fx-data-shared-prod`.udf.fenix_build_to_datetime",
        org_mozilla_fenix_nightly="`moz-fx-data-shared-prod`.udf.fenix_build_to_datetime",  # noqa
        org_mozilla_firefox="`moz-fx-data-shared-prod`.udf.fenix_build_to_datetime",
        org_mozilla_firefox_beta="`moz-fx-data-shared-prod`.udf.fenix_build_to_datetime",  # noqa
        org_mozilla_fennec_aurora="`moz-fx-data-shared-prod`.udf.fenix_build_to_datetime",  # noqa
    )
    if not build_date_udf_mapping.get(args.prefix):
        raise ValueError(f"build date udf for {args.prefix} was not found")

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
            **dict(source_table=f"{args.prefix}_stable.baseline_v1"),
        ),
        init(
            "clients_scalar_aggregates_v1",
            **models.clients_scalar_aggregates(
                source_table=(
                    f"glam_etl.{args.prefix}__view_clients_daily_scalar_aggregates_v1"
                ),
                destination_table=(
                    f"glam_etl.{args.prefix}__clients_scalar_aggregates_v1"
                ),
            ),
        ),
        table(
            "clients_scalar_aggregates_v1",
            **models.clients_scalar_aggregates(
                source_table=(
                    f"glam_etl.{args.prefix}__view_clients_daily_scalar_aggregates_v1"
                ),
                destination_table=(
                    f"glam_etl.{args.prefix}__clients_scalar_aggregates_v1"
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
            "scalar_bucket_counts_v1",
            **models.scalar_bucket_counts(
                source_table=f"glam_etl.{args.prefix}__clients_scalar_aggregates_v1"
            ),
        ),
        table(
            "histogram_bucket_counts_v1",
            **models.histogram_bucket_counts(
                source_table=f"glam_etl.{args.prefix}__clients_histogram_aggregates_v1"
            ),
        ),
        table(
            "probe_counts_v1",
            query_name_prefix="scalar",
            **models.probe_counts(
                source_table=f"glam_etl.{args.prefix}__scalar_bucket_counts_v1",
                is_scalar=True,
            ),
        ),
        table(
            "probe_counts_v1",
            query_name_prefix="histogram",
            **models.probe_counts(
                source_table=f"glam_etl.{args.prefix}__histogram_bucket_counts_v1",
                is_scalar=False,
            ),
        ),
        table(
            "scalar_percentiles_v1",
            **models.scalar_percentiles(
                source_table=f"glam_etl.{args.prefix}__clients_scalar_aggregates_v1"
            ),
        ),
        view("view_clients_daily_scalar_aggregates_v1"),
        view("view_clients_daily_histogram_aggregates_v1"),
        table("histogram_percentiles_v1"),
        view("view_probe_counts_v1"),
        view("view_user_counts_v1", **models.user_counts()),
        table(
            "extract_user_counts_v1", build_date_udf=build_date_udf_mapping[args.prefix]
        ),
        table(
            "extract_probe_counts_v1",
            build_date_udf=build_date_udf_mapping[args.prefix],
        ),
    ]


if __name__ == "__main__":
    main()
