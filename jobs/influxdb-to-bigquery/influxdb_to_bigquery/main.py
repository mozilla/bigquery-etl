from google.cloud import bigquery
from influxdb import InfluxDBClient
import pandas as pd
import click
from datetime import datetime
import logging


def collect_influxdb_data(
    influxdb_host,
    influxdb_port,
    influxdb_username,
    influxdb_password,
    influxdb_measurement,
    date,
    bq_project_id,
    bq_dataset_id,
    bq_table_id,
):
    # Create InfluxDB client and extract data
    client = InfluxDBClient(
        host=influxdb_host,
        port=influxdb_port,
        username=influxdb_username,
        password=influxdb_password,
        ssl=True,
        verify_ssl=True,
    )
    query_template = "SELECT  * FROM {influxdb_measurement} WHERE time >= '{date}T00:00:00Z' AND time < '{date}T23:59:59Z' AND \"environment\"='prod' "  # noqa: E501,E261
    query = query_template.format(date=date, influxdb_measurement=influxdb_measurement)
    results = client.query(query)

    # Convert the resultset to pd dataframe
    df = pd.DataFrame(list(results.get_points()))
    if len(df) > 0:
        # rename the columns to be compatible with BQ
        df.columns = df.columns.str.replace(".", "_")
        df["time"] = pd.to_datetime(df["time"])
        df["submission_date"] = df["time"].apply(lambda x: x.date())
        load_bigquery_table(df, bq_project_id, bq_dataset_id, bq_table_id, date)
    else:
        logging.info(f"{influxdb_measurement} is empty".format(influxdb_measurement))


def load_bigquery_table(df, bq_project_id, bq_dataset_id, bq_table_id, date):
    # Create BigQuery client and insert data
    bq_client = bigquery.Client(project=bq_project_id)
    bq_dataset_ref = bq_client.dataset(bq_dataset_id)
    bq_table_ref = bq_dataset_ref.table(bq_table_id)

    # Configure BigQuery job and write dataframe to table
    job_config = bigquery.LoadJobConfig(
        schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.table.TimePartitioning(field="submission_date"),
    )
    partition = f"{bq_table_ref}${str(date).replace('-', '')}"
    job = bq_client.load_table_from_dataframe(df, partition, job_config=job_config)
    job.result()


@click.command()
@click.option("--bq_project_id", help="GCP BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="GCP BigQuery dataset id", required=True)
@click.option(
    "--bq_table_id",
    help="GCP BigQuery table id",
    required=True,
)
@click.option("--influxdb_measurement", help="Influx measurement to fetch", required=True)
@click.option(
    "--influxdb_username",
    help="Influxdb username",
    required=True,
)
@click.option(
    "--influxdb_password",
    help="Influxdb password",
    required=True,
)
@click.option(
    "--influxdb_host",
    help="Influxdb host URL",
    required=True,
)
@click.option("--influxdb_port", default=8086, help="Influxdb port")
@click.option(
    "--date", type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(), required=True
)
def main(
    bq_project_id,
    bq_dataset_id,
    bq_table_id,
    influxdb_measurement,
    influxdb_username,
    influxdb_password,
    influxdb_host,
    influxdb_port,
    date,
):
    collect_influxdb_data(
        influxdb_host,
        influxdb_port,
        influxdb_username,
        influxdb_password,
        influxdb_measurement,
        date,
        bq_project_id,
        bq_dataset_id,
        bq_table_id,
    )


if __name__ == "__main__":
    main()
