import datetime
import re
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Set
from uuid import uuid4
from zipfile import ZipFile

import click
from google.cloud import bigquery

load_job_config = bigquery.LoadJobConfig(
    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    autodetect=True,
    skip_leading_rows=2,
    source_format=bigquery.SourceFormat.CSV,
    quote_character="",
)


def load_csv(
    bq_client: bigquery.Client, file_path: str, dataset_id: str, table_suffix: str
) -> str:
    with open(file_path, "rb") as f:
        # first line is a title e.g. Fee-Orders reports from 12-01-2020 to 12-31-2020
        title = f.readline().decode(encoding="utf-8").strip()

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

    load_job.result()

    return table_name


def load_temp_tables(
    bq_client: bigquery.Client, source_dir: Path, tmp_dataset: str, table_suffix: str
) -> Set[str]:
    table_names = set()

    zip_files = sorted(source_dir.glob("*.zip"))

    with TemporaryDirectory() as tmpdir:
        for zip_file_path in zip_files:
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

    return table_names


def run_transformation_query(
    project: str,
    src_table: str,
    dst_table: str,
    updated_fieldnames: Dict[str, str],
    date: datetime.date = None,
    partitioning_field: str = None,
):
    bq_client = bigquery.Client(project=project)

    partition = partitioning_field is not None and date is not None

    if partition:
        dst_table = f"{dst_table}${date.strftime('%Y%m%d')}"

    print(f"Copying {src_table} to {dst_table}")

    select_fields = ",\n\t".join(
        [f"{old} AS {updated}" for old, updated in updated_fieldnames.items()]
    )

    query_string = transform_query_template.format(
        fields=select_fields,
        table_id=src_table,
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

    query_job = bq_client.query(query_string, job_config=query_config)
    query_job.result()


# Transform tables to clean up column names, add time partitioning, and
transform_query_template = """
SELECT
  {fields}
FROM
  {table_id}
WHERE
  {date_filter}
"""

date_query_template = """
SELECT
  DISTINCT(DATE({date_field})) AS date
FROM
  {table_id}
ORDER BY
  1
"""


@click.command()
@click.option("--project", required=True)
@click.option("--dst-dataset", required=True)
@click.option("--tmp-dataset", required=True)
@click.option("--source-dir", required=True, type=Path)
def load_reports(project, dst_dataset, source_dir, tmp_dataset):
    bq_client = bigquery.Client(project=project)

    table_suffix = str(uuid4()).replace(
        "-", ""
    )  # add suffix to temp tables to ensure idempotency
    table_names = load_temp_tables(bq_client, source_dir, tmp_dataset, table_suffix)

    for table_name in table_names:
        tmp_table = bq_client.get_table(f"{tmp_dataset}.{table_name}")
        partitioning_field = None

        updated_fields = {
            field.name: re.sub(r"_+$", "", field.name).replace("__", "_").lower()
            for field in tmp_table.schema
        }

        if "date" in updated_fields.values():
            partitioning_field = "date"
        elif "date_shipped" in updated_fields.values():
            partitioning_field = "date_shipped"

        dst_table = re.sub(r"^tmp_", "amazon_", table_name).replace(
            f"_{table_suffix}", ""
        )
        dst_table = f"{project}.{dst_dataset}.{dst_table}"

        # Create partial to use with process pool
        run_query_for_date = partial(
            run_transformation_query,
            project,
            str(tmp_table.reference),
            dst_table,
            updated_fields,
            partitioning_field=partitioning_field,
        )

        # Get dates to query one by one in order to overwrite partitions
        if partitioning_field is not None:
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

        bq_client.delete_table(tmp_table)


if __name__ == "__main__":
    load_reports()
