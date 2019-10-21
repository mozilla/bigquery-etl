#!/usr/bin/python3

"""Read a table from the BigQuery Storage API and write it as parquet."""

from argparse import ArgumentParser
from textwrap import dedent
import json
import re
import sys

try:
    from google.cloud import bigquery
except ImportError as e:
    bigquery = None
    bigquery_error = e

parser = ArgumentParser(description=__doc__)
parser.add_argument("table", help="BigQuery table to read")
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


def maps_from_entries(field, transform_layer=0, *prefix):
    """Generate spark SQL to recursively convert repeated key-value structs to maps."""
    result = full_name = ".".join(prefix + (field.name,))
    if field.field_type == "RECORD":
        repeated = field.mode == "REPEATED"
        if repeated:
            if transform_layer > 0:
                prefix = (f"_{transform_layer}",)
            else:
                prefix = ("_",)
            transform_layer += 1
        else:
            prefix = (*prefix, field.name)
        fields = ", ".join(
            maps_from_entries(subfield, transform_layer, *prefix)
            for subfield in field.fields
        )
        if "(" in fields:
            result = f"STRUCT({fields})"
            if repeated:
                result = f"TRANSFORM({full_name}, {prefix[0]} -> {result})"
        if repeated and {"key", "value"} == {f.name for f in field.fields}:
            result = f"MAP_FROM_ENTRIES({result})"
    return f"{result} AS {field.name}"


def find_maps_from_entries(table):
    """Get maps_from_entries expressions for all maps in the given table."""
    if bigquery is None:
        return ["..."]
    schema = sorted(bigquery.Client().get_table(table).schema, key=lambda f: f.name)
    replace = []
    for index, field in enumerate(schema):
        try:
            expr = maps_from_entries(field)
        except Exception:
            json_field = json.dumps(field.to_api_repr(), indent=2)
            print(f"problem with field {index}:\n{json_field}", file=sys.stderr)
            raise
        if "(" in expr:
            replace += [expr]
    return replace


def main():
    """Main."""
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

    if args.maps_from_entries:
        args.replace += find_maps_from_entries(f"{args.dataset}.{args.table}")

    if args.dry_run:
        replace = f"{args.replace!r}"
        if len(replace) > 60:
            replace = (
                "["
                + ",".join(f"\n{' '*4*5}{expr!r}" for expr in args.replace)
                + f"\n{' '*4*4}]"
            )
        print(
            dedent(
                f"""
                df = (
                    SparkSession.builder.appName('export_to_parquet')
                    .getOrCreate()
                    .read.format('bigquery')
                    .option('dataset', {args.dataset!r})
                    .option('table', {args.table!r})
                    .option('filter', {args.filter!r})
                    .option("parallelism", 0)  # let BigQuery storage API decide
                    .load()
                    .where({args.where!r})
                    .selectExpr(*{args.select!r})
                    .with
                    .drop(*{args.drop!r})
                )
                for sql in {replace}:
                    value, name = re.fullmatch("(?i)(.*) AS (.*)", sql).groups()
                    df = df.withColumn(name, expr(value))
                (
                    df.write.mode({args.write_mode!r})
                    .partitionBy(*{args.partition_by!r})
                    .parquet({args.destination!r})
                )
                """
            ).strip()
        )
    else:
        # delay import to allow --dry-run without spark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import expr

        if bigquery is None:
            raise bigquery_error

        # run spark job from parsed args
        df = (
            SparkSession.builder.appName("export_to_parquet")
            .getOrCreate()
            .read.format("bigquery")
            .option("dataset", args.dataset)
            .option("table", args.table)
            .option("filter", args.filter)
            .option("parallelism", 0)  # let BigQuery storage API decide
            .load()
            .where(args.where)
            .selectExpr(*args.select)
            .drop(*args.drop)
        )
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
