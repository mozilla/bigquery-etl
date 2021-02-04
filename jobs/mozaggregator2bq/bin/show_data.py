#!/usr/bin/env python3
import click
from pyspark.sql import SparkSession


@click.command()
@click.argument("path", type=click.Path(file_okay=False))
def main(path):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(path)
    df.printSchema()
    df.show()


if __name__ == "__main__":
    main()
