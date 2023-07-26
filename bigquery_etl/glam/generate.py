"""Generate templated views."""
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from jinja2 import Environment, PackageLoader, TemplateNotFound
from jsonschema import validate

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.glam import models


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

    template_filename = template_name.split("__")[-1]
    if query_name_prefix:
        table_id = f"{args.prefix}__{query_name_prefix}_{template_filename}"
    else:
        table_id = f"{args.prefix}__{template_filename}"

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
    parser.add_argument("--project", default="glam-fenix-dev")
    parser.add_argument("--dataset", default="glam_etl")
    parser.add_argument("--sql-root", default="sql/")
    parser.add_argument("--daily-view-only", action="store_true", default=False)
    args = parser.parse_args()

    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))

    dataset_path = Path(args.sql_root) / args.project / args.dataset
    if not dataset_path.is_dir():
        raise NotADirectoryError(f"path to {dataset_path} not found")

    # curry functions for convenience
    template = partial(
        from_template, environment=env, dataset_path=dataset_path, args=args
    )
    view = partial(template, QueryType.VIEW)
    table = partial(template, QueryType.TABLE)
    init = partial(template, QueryType.INIT)

    # If this is a logical app id, generate it. Assert that the daily view for
    # the app exists. This assumes that both scalar and histogram aggregates
    # exist and will break down in the case where a glean app only contains one
    # of the scalar or histogram view.
    for daily_view in [
        "view_clients_daily_scalar_aggregates_v1",
        "view_clients_daily_histogram_aggregates_v1",
    ]:
        try:
            view(f"logical_app_id/{args.prefix}__{daily_view}")
        except TemplateNotFound:
            print(f"{args.prefix} is not a logical app id")
            # generate the view for the app id directly
            view(daily_view)

        if not (dataset_path / f"{args.prefix}__{daily_view}").is_dir():
            raise ValueError(f"missing {daily_view}")

    # exit early if we're only generating a daily view
    if args.daily_view_only:
        return

    # Supported fenix/firefox for android products. These are logical ids that
    # are formed from the union of several app_ids (sometimes across date
    # boundaries).
    config_schema = {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "properties": {
                "build_date_udf": {
                    "type": "string",
                    "description": (
                        "The fully qualified path to a UDF that accepts the "
                        "client_info.app_build_id field and returns a datetime."
                    ),
                },
                "filter_version": {
                    "type": "boolean",
                    "description": (
                        "Whether the integer extracted from the "
                        "client_info.app_display_version should be used to "
                        "filter incremental aggregates."
                    ),
                },
                "num_versions_to_keep": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "The number of versions to keep.",
                },
                "total_users": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "The number of users to filter the data on.",
                },
                "minimum_client_count": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "The minimum client count for each build id."
                    "We generally want this to be roughly 0.5% of WAU."
                    "For context see https://github.com/mozilla/glam/issues/1575#issuecomment-946880387",  # noqa E501
                },
            },
            "required": [
                "build_date_udf",
                "filter_version",
                "num_versions_to_keep",
                "total_users",
            ],
        },
    }
    config = {
        "org_mozilla_fenix_glam_nightly": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 10,
            "minimum_client_count": 800,
        },
        "org_mozilla_fenix_glam_beta": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 10,
            "minimum_client_count": 2000,
        },
        "org_mozilla_fenix_glam_release": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 10,
            "minimum_client_count": 90000,
        },
        "firefox_desktop_glam_nightly": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 10,
            "minimum_client_count": 100,
        },
        "firefox_desktop_glam_beta": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 100,
            "minimum_client_count": 1000,
        },
        "firefox_desktop_glam_release": {
            "build_date_udf": "mozfun.glam.build_hour_to_datetime",
            "filter_version": True,
            "num_versions_to_keep": 3,
            "total_users": 100,
            "minimum_client_count": 100000,
        },
    }

    channel_prefixes = {
        "firefox_desktop_glam_nightly": "nightly",
        "firefox_desktop_glam_beta": "beta",
        "firefox_desktop_glam_release": "release",
        "org_mozilla_fenix_glam_nightly": "nightly",
        "org_mozilla_fenix_glam_beta": "beta",
        "org_mozilla_fenix_glam_release": "release",
    }
    validate(instance=config, schema=config_schema)

    if not config.get(args.prefix, {}).get("build_date_udf"):
        raise ValueError(f"build date udf for {args.prefix} was not found")

    [
        table(
            "latest_versions_v1",
            **dict(app_id_channel=(f"'{channel_prefixes[args.prefix]}'")),
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
                **config[args.prefix],
            ),
        ),
        init(
            "clients_histogram_aggregates_v1",
            **models.clients_histogram_aggregates(parameterize=True),
        ),
        table(
            "clients_histogram_aggregates_v1",
            **models.clients_histogram_aggregates(
                parameterize=True, **config[args.prefix]
            ),
        ),
        table(
            "scalar_bucket_counts_v1",
            **models.scalar_bucket_counts(
                source_table=f"glam_etl.{args.prefix}__clients_scalar_aggregates_v1",
                **config[args.prefix],
            ),
        ),
        table(
            "histogram_bucket_counts_v1",
            **models.histogram_bucket_counts(
                source_table=f"glam_etl.{args.prefix}__clients_histogram_aggregates_v1",
                **config[args.prefix],
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
        table("histogram_percentiles_v1"),
        view("view_probe_counts_v1"),
        view("view_user_counts_v1", **models.user_counts()),
        view("view_sample_counts_v1", **models.sample_counts()),
        table("extract_user_counts_v1", **config[args.prefix]),
        table("extract_probe_counts_v1", **config[args.prefix]),
    ]


if __name__ == "__main__":
    main()
