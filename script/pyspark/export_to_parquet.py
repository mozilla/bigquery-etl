#!/usr/bin/python

"""Read a table from the BigQuery Storage API and write it as parquet."""

from argparse import ArgumentParser
from textwrap import dedent
import re


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "table", help='Passed to spark.read.format("bigquery").option("table", _).'
)
parser.add_argument(
    "--dataset",
    dest="dataset",
    default="telemetry",
    help='Passed to spark.read.format("bigquery").option("dataset", _). Defaults to'
    ' "telemetry".',
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
    " --select and --where are processed.",
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
    " Passed to df.selectExpr(*_) before --drop is processed and after --where"
    " is processed. Can include sql expressions",
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
args.partition_by = [f for f in args.partition_by if f not in args.static_partitions]

# add --static-partitions fields to --drop
args.drop += args.static_partitions.keys()

# convert --filter to a single string
args.filter = " AND ".join(args.filter)

if args.dry_run:
    print(
        dedent(
            """
            (
                SparkSession.builder.appName('export_to_parquet')
                .getOrCreate()
                .read.format('bigquery')
                .option('dataset', {dataset!r})
                .option('table', {table!r})
                .option('filter', {filter!r})
                .load()
                .where({where!r})
                .selectExpr(*{select!r})
                .drop(*{drop!r})
                .write.mode({write_mode!r})
                .partitionBy(*{partition_by!r})
                .parquet({destination!r})
            )
            """
        )
        .strip()
        .format(**vars(args))
    )
else:
    # delay import to allow --dry-run without spark
    from pyspark.sql import SparkSession

    # run spark job from parsed args
    (
        SparkSession.builder.appName("export_to_parquet")
        .getOrCreate()
        .read.format("bigquery")
        .option("dataset", args.dataset)
        .option("table", args.table)
        .option("filter", args.filter)
        .load()
        .where(args.where)
        .selectExpr(*args.select)
        .drop(*args.drop)
        .write.mode(args.write_mode)
        .partitionBy(*args.partition_by)
        .parquet(args.destination)
    )
