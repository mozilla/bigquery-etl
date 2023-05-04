#!/usr/bin/env python3
"""Script to convert `pg_dump` directory data into parquet data. This script
also performs transformations to make the resulting aggregates easier to query
within Spark and BigQuery."""

import click
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# This schema is an intermediate schema that is used
METADATA_SCHEMA = T.StructType(
    [
        T.StructField("aggregate_type", T.StringType(), False),
        T.StructField("ds_nodash", T.StringType(), False),
        T.StructField("table_id", T.IntegerType(), False),
    ]
)

DIMENSION_SCHEMA = T.StructType(
    [
        T.StructField("os", T.StringType()),
        T.StructField("child", T.StringType()),
        T.StructField("label", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("osVersion", T.StringType()),
        T.StructField("application", T.StringType()),
        T.StructField("architecture", T.StringType()),
    ]
)

AGGREGATE_SCHEMA = T.StringType()


@click.command("pg_dump_to_parquet")
@click.option(
    "--input-dir",
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help="The input directory containing a dump of the mozaggregator database.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    required=True,
    help="The output directory to be overwritten.",
)
def main(input_dir, output_dir):
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = read_pg_dump(spark, input_dir)

    # apply some other transformations to obtain the final table
    result = (
        df.withColumn("parts", F.split("table_name", "_"))
        .withColumn("dimension", F.from_json("dimension", DIMENSION_SCHEMA))
        .select(
            F.current_date().alias("ingest_date"),
            "aggregate_type",
            "ds_nodash",
            # parts[:2] form aggregate_type, this is parsed from the filename
            F.col("parts").getItem(2).alias("channel"),
            F.col("parts").getItem(3).alias("version"),
            # parts[-1] is ds_nodash, this is parsed from the filename
            "dimension.*",
            from_pg_array_udf("aggregate"),
        )
    )
    # -RECORD 0-----------------------------------------------------
    # aggregate_type | submission_date
    # channel        | nightly
    # version        | 43
    # ds_nodash      | 20191201
    # os             | Windows_NT
    # child          | false
    # label          |
    # metric         | A11Y_INSTANTIATED_FLAG
    # osVersion      | 10.0
    # application    | Firefox
    # architecture   | x86
    # aggregate      | "[0, 2, 0, 2, 2]"

    result.repartition(1).write.parquet(output_dir, mode="overwrite")


def read_pg_dump(spark, input_dir):
    """Read gzipped pg_dump data.

    -RECORD 0-----------------------------------------------------------------
    table_name     | submission_date_nightly_43_20191201
    aggregate_type | submission_date
    ds_nodash      | 20191201
    dimension      | {"os": "Windows_NT", "child": "false", "label": ""...
    aggregate      | {0,2,0,2,2}
    """

    # parse the table of contents
    toc_file = f"{input_dir}/toc.dat"
    with open(toc_file, "rb") as f:
        data = f.read()
    toc_df = spark.createDataFrame([Row(**d) for d in parse_toc(data)])

    data_df = (
        spark.read.csv(
            f"{input_dir}/*.dat.gz",
            sep="\t",
            schema="dimension string, aggregate string",
        )
        .withColumn("file_name", parse_filename(F.input_file_name()))
        .select("dimension", "aggregate", "file_name.*")
    )

    return data_df.join(toc_df, on="table_id", how="inner")


@F.udf(METADATA_SCHEMA)
def parse_filename(path):
    """Parse the convention for the directory structure to obtain the join key
    and other associated metadata.

        ../data
        ├── build_id
        │   └── 20191201
        │       ├── 474306.dat.gz
        │       └── toc.dat
        └── submission_date
            └── 20191201
                ├── 474405.dat.gz
                ├── 474406.dat.gz
                ...
                ├── 474504.dat.gz
                └── toc.dat
    """
    aggregate_type, ds_nodash, filename = path.split("/")[-3:]
    return aggregate_type, ds_nodash, int(filename.split(".")[0])


def parse_toc(data):
    return [extract_toc_mapping(line) for line in data.split(b"\n") if b".dat" in line]


def extract_toc_mapping(line):
    """Parse the binary toc files for the table and the table name."""

    # We rely on the padding in the binary file to extract the necessary information
    # b'\x00\x00\x00474424.dat\x00%=\x07\x00\x00\x01\x00\x00\x00\x00\x01\x00\x00\x000
    # \x00\x08\x00\x00\x0090014321\x00"\x00\x00\x00submission_date_aurora_42_20191201\x00'
    #
    # b'\x00\x00\x00496167.dat\x00\xb9\x91\x07\x00\x00\x00\x00\x00\x00\x00
    # \x04\x00\x00\x001259\x00\x08\x00\x00\x0091420008\x00+\x00\x00\x00
    # build_id_nightly_68_20191220_dimensions_idx
    # \x00\x05\x00\x00\x00INDEX\x00\x04\x00\x00\x00\x00\x87\x00\x00\x00 CREATE
    # INDEX build_id_nightly_68_20191220_dimensions_idx ON
    # public.build_id_nightly_68_20191220 USING gin (dimensions
    # jsonb_path_ops);'
    processed = line.replace(b"\x00", b" ").strip().split()

    table_name = processed[-1].decode()
    if b"CREATE INDEX" in line:
        # this is an indexed table, get the actual name
        for element in processed:
            if b"public." not in element:
                continue
            table_name = element.split(b"public.")[-1].decode()

    # [b'474455.dat', b'(=\x07', b'\x01', b'\x01', b'0', b'\x08', b'90014330', b'"',
    # b'submission_date_aurora_40_20191201']
    return {"table_id": processed[0].split(b".")[0].decode(), "table_name": table_name}


def from_pg_array_udf(arr):
    return F.translate(F.translate(arr, "{", "["), "}", "]").alias(arr)


if __name__ == "__main__":
    main()
