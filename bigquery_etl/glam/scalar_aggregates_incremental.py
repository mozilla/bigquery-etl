r"""Generate a query for incremental processing of scalar aggregates.

Telemetry usage:

```bash
python -m bigquery_etl.glam.scalar_aggregates_incremental --init \
    > sql/telemetry_derived/clients_scalar_aggregates_v1/init.sql

python -m bigquery_etl.glam.scalar_aggregates_incremental \
    > sql/telemetry_derived/clients_scalar_aggregates_v1/query.sql
```

Glean usage:

``bash
python -m bigquery_etl.glam.scalar_aggregates_incremental \
    --ping-type glean \
    --source fenix_clients_daily_scalar_aggregates_v1 \
    --destination fenix_clients_scalar_aggregates_v1 \
    > sql/glam_etl/fenix_clients_scalar_aggregates_v1/query.sql
```
"""
from argparse import ArgumentParser
from typing import List

from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat


def render_main(
    header: str,
    user_data_type: str,
    user_data_attributes: List[str],
    attributes: List[str],
    extract_select_clause: str,
    join_filter: str,
    source_table: str,
    destination_table: str,
) -> str:
    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))
    main_sql = env.get_template("clients_scalar_aggregates_v1.sql")
    return reformat(
        main_sql.render(
            header=header,
            user_data_type=user_data_type,
            user_data_attributes=",".join(user_data_attributes),
            attributes=",".join(attributes),
            attributes_list=attributes,
            extract_select_clause=extract_select_clause,
            join_filter=join_filter,
            source_table=source_table,
            destination_table=destination_table,
        )
    )


def render_init(
    header, destination_table, attributes, user_data_type, partition_clause, **kwargs
) -> str:
    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))
    init_sql = env.get_template("clients_scalar_aggregates_v1.init.sql")
    return reformat(
        init_sql.render(
            header=header,
            destination_table=destination_table,
            attributes=",".join(attributes),
            user_data_type=user_data_type,
            partition_clause=partition_clause,
        )
    )


def telemetry_variables():
    return dict(
        user_data_type="""
            ARRAY<
                STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                process STRING,
                agg_type STRING,
                value FLOAT64
                >
            >
        """,
        user_data_attributes=[
            "metric",
            "metric_type",
            "key",
            "process",
            # [agg_type, value] are shared between telemetry and glean
        ],
        attributes=["client_id", "os", "app_version", "app_build_id", "channel"],
        extract_select_clause=f"""
            * EXCEPT(app_version),
            CAST(app_version AS INT64) as app_version
        """,
        join_filter="""
            LEFT JOIN
                latest_versions
            USING
                (channel)
            WHERE
                app_version >= (latest_version - 2)
        """,
        partition_clause="""
            PARTITION BY
                RANGE_BUCKET(app_version, GENERATE_ARRAY(30, 200, 1))
        """,
    )


def glean_variables():
    return dict(
        # does not include "process" field
        user_data_type="""
            ARRAY<
                STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                agg_type STRING,
                value FLOAT64
                >
            >
        """,
        user_data_attributes=[
            "metric",
            "metric_type",
            "key",
            # [agg_type, value] are shared between telemetry and glean
        ],
        attributes=[
            "client_id",
            "ping_type",
            "os",
            "app_version",
            "app_build_id",
            "channel",
        ],
        extract_select_clause=f"""
            * EXCEPT(app_version),
            CAST(app_version AS INT64) as app_version
        """,
        # no filtering on channel
        join_filter="",
        partition_clause="",
    )


def main():
    """Generate `telemetry_derived.clients_scalar_aggregates_v1`."""

    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "--source",
        default="telemetry_derived.clients_daily_scalar_aggregates_v1",
        help="Source `clients_daily_scalar_aggregates` table",
    )
    parser.add_argument(
        "--destination",
        default="telemetry_derived.clients_scalar_aggregates_v1",
        help="Destination for incremental `clients_scalar_aggregates` table",
    )
    parser.add_argument(
        "--ping-type",
        default="telemetry",
        choices=["glean", "telemetry"],
        help="determine attributes and user data types to aggregate",
    )
    parser.add_argument(
        "--init", action="store_true", help="generate init.sql instead of query.sql"
    )
    args = parser.parse_args()

    module_name = "bigquery_etl.glam.scalar_aggregates_incremental"
    header = f"-- generated by: python3 -m {module_name}"
    if args.init:
        header += f" --init --source {args.source} --ping-type {args.ping_type}"
    else:
        header += " " + " ".join(
            [f"--{k} {v}" for k, v in vars(args).items() if k != "init"]
        )
    variables = (
        telemetry_variables() if args.ping_type == "telemetry" else glean_variables()
    )

    render = render_init if args.init else render_main
    rendered = render(
        source_table=args.source,
        destination_table=args.destination,
        header=header,
        **variables,
    )
    print(rendered)


if __name__ == "__main__":
    main()
