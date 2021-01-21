#!/usr/bin/env python3

"""Exports experiment monitoring data to GCS as JSON."""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from functools import partial
from google.cloud import storage
from google.cloud import bigquery
from multiprocessing import Pool
import random
import smart_open
import string

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--source_project",
    "--source-project",
    default="moz-fx-data-shared-prod",
    help="Project containing datasets to be exported",
)
parser.add_argument(
    "--destination_project",
    "--destination-project",
    default="moz-fx-data-experiments",
    help="Project with bucket data is exported to",
)
parser.add_argument(
    "--bucket", default="mozanalysis", help="GCS bucket data is exported to"
)
parser.add_argument(
    "--gcs_path", "--gcs-path", default="monitoring", help="GCS path data is written to"
)
parser.add_argument(
    "--datasets",
    help="Experiment monitoring datasets to be exported",
    nargs="*",
    default=[
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_daily_active_population",  # noqa E501
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_cumulative_population_estimate",  # noqa E501
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_other_events_overall",  # noqa E501
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_overall",
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_unenrollment_overall",  # noqa E501
    ],
)


def get_active_experiments(client, date, dataset):
    """
    Determine active experiments.

    Experiments are considered as active if they are currently live or have been live
    within the past 14 days.
    Returns the experiment slug and experiment start date.
    """
    job = client.query(
        f"""
        SELECT DISTINCT experiment, start_date
        FROM `{dataset}`
        LEFT JOIN (
            SELECT
                start_date,
                end_date,
                normandy_slug
            FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
        )
        ON experiment = normandy_slug
        WHERE TIMESTAMP_ADD(TIMESTAMP(end_date), INTERVAL 14 DAY) >= TIMESTAMP('{date}')
        """
    )

    result = job.result()
    return [
        (row.experiment, datetime.combine(row.start_date, datetime.min.time()))
        for row in result
    ]


def export_data_for_experiment(
    date, dataset, bucket, gcs_path, source_project, destination_project, experiment
):
    """Export the monitoring data for a specific experiment in the dataset."""
    experiment_slug, start_date = experiment
    table_name = dataset.split(".")[-1]
    storage_client = storage.Client(destination_project)
    client = bigquery.Client(source_project)

    if (date - start_date) > timedelta(days=14):
        # if the experiment has been running for more than 14 days,
        # export data as 30 minute intervals
        query = f"""
            WITH data AS (
                SELECT * EXCEPT(experiment) FROM
                `{dataset}`
                WHERE experiment = '{experiment_slug}' AND
                time >= (
                    SELECT TIMESTAMP(MIN(start_date))
                    FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
                    WHERE normandy_slug = '{experiment_slug}'
                )
            )
            SELECT * EXCEPT(rn) FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY time) AS rn
                FROM (
                    SELECT
                        * EXCEPT(time),
                        TIMESTAMP_SECONDS(
                            UNIX_SECONDS(time) - MOD(UNIX_SECONDS(time), 30 * 60) + 30 * 60
                            ) AS time,
                    FROM data
                    ORDER BY time DESC
                )
            )
            WHERE rn = 1
            ORDER BY time DESC
        """
    else:
        # for recently launched experiments, data is exported as 5 minuted intervals
        query = f"""
            SELECT * EXCEPT(experiment) FROM {dataset}
            WHERE experiment = '{experiment_slug}' AND
            time >= (
                SELECT TIMESTAMP(MIN(start_date))
                FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
                WHERE normandy_slug = '{experiment_slug}'
            )
            ORDER BY time DESC
        """

    job = client.query(query)
    job.result()
    dataset_ref = bigquery.DatasetReference(source_project, job.destination.dataset_id)
    table_ref = dataset_ref.table(job.destination.table_id)

    # export data for experiment grouped by branch
    _upload_table_to_gcs(
        table_ref,
        bucket,
        gcs_path,
        experiment_slug,
        f"{table_name}_by_branch",
        source_project,
        client,
        storage_client,
    )

    # export aggregated data for experiment
    query = f"""
        SELECT
            time,
            SUM(value) AS value
        FROM `{source_project}.{job.destination.dataset_id}.{job.destination.table_id}`
        GROUP BY 1
        ORDER BY time DESC
    """

    job = client.query(query)
    job.result()
    dataset_ref = bigquery.DatasetReference(source_project, job.destination.dataset_id)
    table_ref = dataset_ref.table(job.destination.table_id)

    _upload_table_to_gcs(
        table_ref,
        bucket,
        gcs_path,
        experiment_slug,
        table_name,
        source_project,
        client,
        storage_client,
    )


def _upload_table_to_gcs(
    table,
    bucket,
    gcs_path,
    experiment_slug,
    table_name,
    source_project,
    client,
    storage_client,
):
    """Export the provided table reference to GCS as JSON."""
    # add a random string to the identifier to prevent collision errors if there
    # happen to be multiple instances running that export data for the same experiment
    tmp = "".join(random.choices(string.ascii_lowercase, k=8))
    destination_uri = (
        f"gs://{bucket}/{gcs_path}/{experiment_slug}_{table_name}_{tmp}.ndjson"
    )

    print(f"Export table {table} to {destination_uri}")

    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = "NEWLINE_DELIMITED_JSON"
    extract_job = client.extract_table(
        table, destination_uri, location="US", job_config=job_config
    )
    extract_job.result()

    # convert ndjson to json
    _convert_ndjson_to_json(
        bucket, gcs_path, experiment_slug, table_name, storage_client, tmp
    )


def export_dataset(
    date, source_project, destination_project, bucket, gcs_path, dataset
):
    """Export monitoring data for all active experiments in the dataset."""
    client = bigquery.Client(source_project)
    active_experiments = get_active_experiments(client, date, dataset)

    print(f"Active experiments: {active_experiments}")

    with Pool(20) as p:
        _experiment_data_export = partial(
            export_data_for_experiment,
            date,
            dataset,
            bucket,
            gcs_path,
            source_project,
            destination_project,
        )
        p.map(_experiment_data_export, active_experiments)


def _convert_ndjson_to_json(
    bucket_name: str,
    target_path: str,
    experiment_slug: str,
    table: str,
    storage_client: storage.Client,
    tmp: str,
):
    """Convert the provided ndjson file on GCS to json."""
    ndjson_blob_path = (
        f"gs://{bucket_name}/{target_path}/{experiment_slug}_{table}_{tmp}.ndjson"
    )
    json_blob_path = (
        f"gs://{bucket_name}/{target_path}/{experiment_slug}_{table}_{tmp}.json"
    )

    print(f"Convert {ndjson_blob_path} to {json_blob_path}")

    # stream from GCS
    with smart_open.open(ndjson_blob_path) as fin:
        first_line = True

        with smart_open.open(json_blob_path, "w") as fout:
            fout.write("[")

            for line in fin:
                if not first_line:
                    fout.write(",")

                fout.write(line.replace("\n", ""))
                first_line = False

            fout.write("]")
            fout.close()
            fin.close()

    # delete ndjson file from bucket
    print(f"Remove file {experiment_slug}_{table}_{tmp}.ndjson")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{target_path}/{experiment_slug}_{table}_{tmp}.ndjson")
    blob.delete()
    print(
        f"Rename file {experiment_slug}_{table}_{tmp}.json to "
        f"{experiment_slug}_{table}.json"
    )
    bucket.rename_blob(
        bucket.blob(f"{target_path}/{experiment_slug}_{table}_{tmp}.json"),
        f"{target_path}/{experiment_slug}_{table}.json",
    )


def main():
    """Run the monitoring data export to GCS."""
    args = parser.parse_args()
    date = datetime.now()

    for dataset in args.datasets:
        export_dataset(
            date,
            args.source_project,
            args.destination_project,
            args.bucket,
            args.gcs_path,
            dataset,
        )


if __name__ == "__main__":
    main()
