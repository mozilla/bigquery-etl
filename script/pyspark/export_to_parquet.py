#!/usr/bin/python3

"""Read a table from BigQuery and write it as parquet."""

import json
import re
import sys
from argparse import ArgumentParser
from textwrap import dedent

try:
    from google.cloud import bigquery
except ImportError as e:
    bigquery = None
    bigquery_error = e

parser = ArgumentParser(description=__doc__)
parser.add_argument("table", help="BigQuery table to read")
parser.add_argument(
    "--avro-path",
    help="Avro export path that should be read instead of BigQuery Storage API.",
)
parser.add_argument(
    "--dataset",
    dest="dataset",
    default="telemetry",
    help='BigQuery dataset that contains TABLE. Defaults to "telemetry".',
)
parser.add_argument(
    "--destination",
    default="s3://telemetry-parquet/",
    help="The path where parquet will be written. This will have table and"
    " --static-partitions appended as directories. If this starts with s3:// it will"
    " be replaced with s3a://.",
)
parser.add_argument(
    "--destination-table",
    default=None,
    help="The name of the destination parquet table. Defaults to the source table name."
    " If this ends with a version by matching /_v[0-9]+$/ it will be converted to"
    " a directory in the output.",
)
parser.add_argument(
    "--drop",
    default=[],
    dest="drop",
    nargs="+",
    help="A list of fields to exclude from the output. Passed to df.drop(*_) after"
    " --select is applied.",
)
parser.add_argument(
    "--dry-run",
    dest="dry_run",
    action="store_true",
    help="Print the spark job that would be run, without running it, and exit.",
)
parser.add_argument(
    "--filter",
    default=[],
    dest="filter",
    nargs="+",
    help="A list of conditions to limit the input before it reaches spark. Passed to"
    ' spark.read.format("bigquery").option("filter", " AND ".join(_)). If a field is'
    " referenced here and later dropped via --drop or --static-partitions then it must"
    " be referenced in --where.",
)
parser.add_argument(
    "--partition-by",
    default=["submission_date"],
    dest="partition_by",
    nargs="+",
    help="A list of fields on which to dynamically partition the output. Passed to"
    ' df.write.partitionBy(*_). Defaults to ["submission_date"]. Fields specified in'
    " --static-partitions will be ignored.",
)
parser.add_argument(
    "--select",
    default=["*"],
    dest="select",
    nargs="+",
    help="A list of all fields to include in the output."
    " Passed to df.selectExpr(*_) after --replace is applied and before --drop."
    " Can include sql expressions.",
)
parser.add_argument(
    "--maps-from-entries",
    action="store_true",
    help="Recursively convert repeated key-value structs with maps.",
)
parser.add_argument(
    "--bigint-columns",
    nargs="*",
    help="A list of columns that should remain BIGINT in the output, while any other "
    " BIGINT columns are converted to INT. If unspecified, all columns remain BIGINT.",
)
parser.add_argument(
    "--replace",
    default=[],
    nargs="+",
    help="A list of expressions that modify columns in the output. Passed to"
    " df.withColumnExpr(_) one at a time after --where applied and before --select."
    " Can include sql expressions.",
)
parser.add_argument(
    "--static-partitions",
    dest="static_partitions",
    default=[],
    nargs="+",
    help="Static partitions specified as FIELD=VALUE that will be appended to"
    " --destination after table, with FIELD dropped from the input.",
)
parser.add_argument(
    "--submission-date",
    dest="submission_date",
    help="Short for: --filter \"submission_date = DATE 'SUBMISSION_DATE'\""
    " --where \"submission_date = DATE 'SUBMISSION_DATE'\""
    " --static-partitions submission_date=SUBMISSION_DATE",
)
parser.add_argument(
    "--where",
    dest="where",
    default="TRUE",
    help="An expression to limit the output. Passed to df.where(_). If a field is"
    " referenced in filter and later dropped via --drop or --static-partitions then it"
    " must be referenced here.",
)
parser.add_argument(
    "--write-mode",
    dest="write_mode",
    default="overwrite",
    help='Passed to df.write.mode(_). Defaults to "overwrite".',
)
parser.add_argument(
    "--partition-overwrite-mode",
    default="STATIC",
    type=str.upper,
    help='Passed to spark.conf.set("spark.sql.sources.partitionOverwriteMode", _).'
    ' Defaults to "STATIC".',
)


def transform_field(
    field, maps_from_entries=False, bigint_columns=None, transform_layer=0, *prefix
):
    """
    Generate spark SQL to recursively convert fields types.

    If maps_from_entries is True, convert repeated key-value structs to maps.

    If bigint_columns is a list, convert non-matching BIGINT columns to INT.
    """
    transformed = False
    result = full_name = ".".join(prefix + (field.name,))
    repeated = field.mode == "REPEATED"
    if repeated:
        if transform_layer > 0:
            prefix = (f"_{transform_layer}",)
        else:
            prefix = ("_",)
        transform_layer += 1
    else:
        prefix = (*prefix, field.name)
    if field.field_type == "RECORD":
        if bigint_columns is not None:
            # get the bigint_columns nested under this field
            prefix_len = len(field.name) + 1
            bigint_columns = [
                column[prefix_len:]
                for column in bigint_columns
                if column.startswith(field.name + ".")
            ]
        subfields = [
            transform_field(
                subfield, maps_from_entries, bigint_columns, transform_layer, *prefix
            )
            for subfield in field.fields
        ]
        if any(subfield_transformed for _, subfield_transformed in subfields):
            transformed = True
            fields = ", ".join(transform for transform, _ in subfields)
            result = f"STRUCT({fields})"
            if repeated:
                result = f"TRANSFORM({full_name}, {prefix[0]} -> {result})"
        if maps_from_entries:
            if repeated and {"key", "value"} == {f.name for f in field.fields}:
                transformed = True
                result = f"MAP_FROM_ENTRIES({result})"
    elif field.field_type == "INTEGER":
        if bigint_columns is not None and field.name not in bigint_columns:
            transformed = True
            if repeated:
                result = f"TRANSFORM({full_name}, {prefix[0]} -> INT({prefix[0]}))"
            else:
                result = f"INT({full_name})"
    return f"{result} AS {field.name}", transformed


def transform_schema(table, maps_from_entries=False, bigint_columns=None):
    """Get maps_from_entries expressions for all maps in the given table."""
    if bigquery is None:
        return ["..."]
    schema = sorted(bigquery.Client().get_table(table).schema, key=lambda f: f.name)
    replace = []
    for index, field in enumerate(schema):
        try:
            expr, transformed = transform_field(
                field, maps_from_entries, bigint_columns
            )
        except Exception:
            json_field = json.dumps(field.to_api_repr(), indent=2)
            print(f"problem with field {index}:\n{json_field}", file=sys.stderr)
            raise
        if transformed:
            replace += [expr]
    return replace


def main():
    """Read a table from BigQuery and write it as parquet."""
    args = parser.parse_args()

    # handle --submission-date
    if args.submission_date is not None:
        # --filter "submission_date = DATE 'SUBMISSION_DATE'"
        condition = "submission_date = DATE '" + args.submission_date + "'"
        args.filter.append(condition)
        # --static-partitions submission_date=SUBMISSION_DATE
        args.static_partitions.append("submission_date=" + args.submission_date)
        # --where "submission_date IS NOT NULL"
        if args.where == "TRUE":
            args.where = condition
        else:
            args.where = "(" + args.where + ") AND " + condition

    # Set default --destination-table if it was not provided
    if args.destination_table is None:
        args.destination_table = args.table

    # append table and --static-partitions to destination
    args.destination = "/".join(
        [
            re.sub("^s3://", "s3a://", args.destination).rstrip("/"),
            re.sub("_(v[0-9]+)$", r"/\1", args.destination_table.rsplit(".", 1).pop()),
        ]
        + args.static_partitions
    )

    # convert --static-partitions to a dict
    args.static_partitions = dict(p.split("=", 1) for p in args.static_partitions)

    # remove --static-partitions fields from --partition-by
    args.partition_by = [
        f for f in args.partition_by if f not in args.static_partitions
    ]

    # add --static-partitions fields to --drop
    args.drop += args.static_partitions.keys()

    # convert --filter to a single string
    args.filter = " AND ".join(args.filter)

    if args.maps_from_entries or args.bigint_columns is not None:
        if "." in args.table:
            table_ref = args.table.replace(":", ".")
        else:
            table_ref = f"{args.dataset}.{args.table}"
        args.replace += transform_schema(
            table_ref, args.maps_from_entries, args.bigint_columns
        )

    if args.dry_run:
        replace = f"{args.replace!r}"
        if len(replace) > 60:
            replace = (
                "["
                + ",".join(f"\n{' '*4*5}{expr!r}" for expr in args.replace)
                + f"\n{' '*4*4}]"
            )
        print("spark = SparkSession.builder.appName('export_to_parquet').getOrCreate()")
        print("")
        print(
            "spark.conf.set('spark.sql.sources.partitionOverwriteMode', "
            f"{args.partition_overwrite_mode!r})"
        )
        print("")
        if args.avro_path is not None:
            print(f"df = spark.read.format('avro').load({args.avro_path!r})")
        else:
            print(
                dedent(
                    f"""
                    df = (
                        spark.read.format('bigquery')
                        .option('dataset', {args.dataset!r})
                        .option('table', {args.table!r})
                        .option('filter', {args.filter!r})
                        .option("parallelism", 0)  # let BigQuery storage API decide
                        .load()
                    )
                    """
                ).strip()
            )
        print("")
        print(
            dedent(
                f"""
                df = df.where({args.where!r}).selectExpr(*{args.select!r}).drop(*{args.drop!r})

                for sql in {replace}:
                    value, name = re.fullmatch("(?i)(.*) AS (.*)", sql).groups()
                    df = df.withColumn(name, expr(value))

                (
                    df.write.mode({args.write_mode!r})
                    .partitionBy(*{args.partition_by!r})
                    .parquet({args.destination!r})
                )
                """  # noqa:E501
            ).strip()
        )
    else:
        # delay import to allow --dry-run without spark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import expr

        if bigquery is None:
            raise bigquery_error

        spark = SparkSession.builder.appName("export_to_parquet").getOrCreate()

        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode", args.partition_overwrite_mode
        )

        # run spark job from parsed args
        if args.avro_path is not None:
            df = spark.read.format("avro").load(args.avro_path)
        else:
            df = (
                spark.read.format("bigquery")
                .option("dataset", args.dataset)
                .option("table", args.table)
                .option("filter", args.filter)
                .option("parallelism", 0)  # let BigQuery storage API decide
                .load()
            )

        df = df.where(args.where).selectExpr(*args.select).drop(*args.drop)

        for sql in args.replace:
            value, name = re.fullmatch("(?i)(.*) AS (.*)", sql).groups()
            df = df.withColumn(name, expr(value))

        (
            df.write.mode(args.write_mode)
            .partitionBy(*args.partition_by)
            .parquet(args.destination)
        )


if __name__ == "__main__":
    main()
