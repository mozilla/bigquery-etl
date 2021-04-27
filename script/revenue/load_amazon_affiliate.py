import datetime
import re
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Set
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

import click
from google.cloud import bigquery, exceptions
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

country_config = {
    "us": {
        "currency": "usd",
        "tables": [
            "bonusearnings",
            "bounty",
            "daily_trends",
            "earnings",
            "orders",
            "orderswithclicks",
        ],
    },
    "uk": {
        "currency": "gbp",
        "tables": ["bounty", "daily_trends", "earnings", "orders"],
    },
    "de": {
        "currency": "eur",
        "tables": ["bounty", "daily_trends", "earnings", "orders"],
    },
}

table_partitioning_fields = {
    "bonusearnings": "date",
    "bounty": "date_shipped",
    "daily_trends": "date",
    "earnings": "date_shipped",
    "orders": "date",
    "orderswithclicks": "date",
}


def load_csv(
    bq_client: bigquery.Client, file_path: str, dataset_id: str, table_suffix: str
) -> str:
    with open(file_path, "rb") as f:
        # first line is a title e.g. Fee-Orders reports from 12-01-2020 to 12-31-2020
        title = f.readline().decode(encoding="utf-8").strip()
        # set placeholder names in load stage
        # schema is manually defined in transform stage
        column_count = len(f.readline().decode(encoding="utf-8").split(","))

        schema = [
            bigquery.SchemaField(name=f"field_{i}", field_type="STRING")
            for i in range(column_count)
        ]

        load_job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            skip_leading_rows=2,
            source_format=bigquery.SourceFormat.CSV,
            quote_character="",
            schema=schema,
            max_bad_records=5,  # files may contain a few invalid ascii characters
        )

        report_type = (
            title.split("from")[0]
            .strip()
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
            .replace("fee_", "")
            .replace("_reports", "")
        )
        table_name = f"tmp_{report_type}_{table_suffix}"

        table_id = f"{dataset_id}.{table_name}"

        print(f"Reading: {title}, Writing to {table_id}")

        load_job = bq_client.load_table_from_file(
            f,
            table_id,
            job_config=load_job_config,
            rewind=True,
        )

    try:
        load_job.result()
    except exceptions.BadRequest as e:
        print(e, file=sys.stderr)

    return table_name


def load_temp_tables(
    bq_client: bigquery.Client, source_dir: Path, tmp_dataset: str, table_suffix: str
) -> Set[str]:
    table_names = set()

    zip_files = sorted(source_dir.glob("*.zip"))

    with TemporaryDirectory() as tmpdir:
        for zip_file_path in zip_files:
            try:
                with ZipFile(zip_file_path) as zip_file:
                    files = zip_file.namelist()

                    if len(files) != 1:
                        print(
                            f"{zip_file_path.name} contains more than one file",
                            file=sys.stderr,
                        )
                        continue

                    data_file = zip_file.extract(files[0], path=tmpdir)
                    table_names.add(
                        load_csv(bq_client, data_file, tmp_dataset, table_suffix)
                    )
            except BadZipFile:
                print(f"{zip_file_path.name} is not a valid zip file", file=sys.stderr)

    return table_names


def run_transformation_query(
    project: str,
    src_table: str,
    dst_table: str,
    query_text: str,
    date: datetime.date = None,
    partitioning_field: str = None,
):
    bq_client = bigquery.Client(project=project)

    partition = partitioning_field is not None and date is not None

    if partition:
        dst_table = f"{dst_table}${date.strftime('%Y%m%d')}"

    print(f"Copying {src_table} to {dst_table}")

    query_text = query_text.format(
        date_filter=f"DATE({partitioning_field}) = '{date}'" if partition else "TRUE",
    )
    query_config = bigquery.QueryJobConfig(
        destination=dst_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    if partitioning_field is not None:
        query_config.time_partitioning = bigquery.TimePartitioning(
            field=partitioning_field,
        )

    query_job = bq_client.query(query_text, job_config=query_config)
    query_job.result()


def transform_temp_tables(
    bq_client: bigquery.Client,
    project: str,
    table_names: Set[str],
    tmp_dataset: str,
    tmp_table_suffix: str,
    dst_dataset: str,
    country: str,
    overwrite: bool,
):
    """Use queries to apply schema on temp tables into final tables."""
    date_query_template = """
    SELECT
      DISTINCT(DATE({date_field})) AS date
    FROM
      {table_id}
    ORDER BY
      1
    """

    template_env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "amazon_load_queries")
    )

    for table_name in table_names:
        tmp_table = bq_client.get_table(f"{tmp_dataset}.{table_name}")

        report_type = re.sub(r"^tmp_", "", table_name).replace(
            f"_{tmp_table_suffix}", ""
        )

        if report_type not in country_config[country]["tables"]:
            print(f"Skipping {report_type} table for country {country}")
            continue

        dst_table = f"{project}.{dst_dataset}.amazon_{report_type}_{country}"

        try:
            query_template = template_env.get_template(f"{country}_{report_type}.sql")
        except TemplateNotFound:
            query_template = template_env.get_template(f"default_{report_type}.sql")

        query_text = query_template.render(
            currency=country_config[country]["currency"],
            table_id=str(tmp_table.reference),
        )

        partitioning_field = table_partitioning_fields[report_type]

        # Create partial to use with process pool
        run_query_for_date = partial(
            run_transformation_query,
            project,
            str(tmp_table.reference),
            dst_table,
            query_text,
            partitioning_field=partitioning_field,
        )

        # Get dates to query one by one in order to overwrite partitions
        if not overwrite and partitioning_field is not None:
            date_query_string = date_query_template.format(
                date_field=partitioning_field,
                table_id=tmp_table.reference,
            )
            date_query_job = bq_client.query(date_query_string)
            date_query_results = date_query_job.result()
            distinct_dates = [row["date"] for row in date_query_results]

            with Pool(20) as pool:
                pool.map(run_query_for_date, distinct_dates)
        else:
            run_query_for_date(date=None)


@click.command()
@click.option("--project", required=True)
@click.option("--dst-dataset", required=True)
@click.option("--tmp-dataset")
@click.option("--source-dir", required=True, type=Path)
@click.option(
    "--country",
    required=True,
    type=click.Choice(["us", "uk", "de"], case_sensitive=False),
)
@click.option("--overwrite/--no-overwrite", help="Overwrite entire destination table")
def load_reports(project, dst_dataset, source_dir, tmp_dataset, country, overwrite):
    bq_client = bigquery.Client(project=project)

    tmp_table_suffix = str(uuid4()).replace(
        "-", ""
    )  # add suffix to temp tables to ensure idempotency
    table_names = load_temp_tables(bq_client, source_dir, tmp_dataset, tmp_table_suffix)

    try:
        transform_temp_tables(
            bq_client,
            project,
            table_names,
            tmp_dataset,
            tmp_table_suffix,
            dst_dataset,
            country,
            overwrite,
        )
    finally:
        print("Deleting temporary tables")
        for tmp_table in table_names:
            bq_client.delete_table(f"{project}.{tmp_dataset}.{tmp_table}")


if __name__ == "__main__":
    load_reports()
