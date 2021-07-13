#!/usr/bin/env python3

"""
Import iProspect data from moz-fx-data-marketing-prod.iprospect.adspend_raw_v1.

The raw table represents the data in the CSV files that are updated daily and
contain the last 30 days of data. This script will import and update data for
the specified date and the 30 days prior to that date into BigQuery.
"""

from argparse import ArgumentParser
from google.cloud import bigquery
from multiprocessing.pool import ThreadPool

import datetime

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-marketing-prod")
parser.add_argument("--dataset", default="iprospect")
parser.add_argument("--table", default="adspend_v1")
parser.add_argument("--parallelism", default=8)


def main():
    """Load CSV data to temporary table."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    end_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    date_list = [
        datetime.datetime.strftime(end_date - datetime.timedelta(days=x), "%Y-%m-%d")
        for x in range(30)
    ]

    def import_data(active_date):
        partition = active_date.replace("-", "")
        destination = f"{args.project}.{args.dataset}.{args.table}${partition}"

        job_config = bigquery.QueryJobConfig(
            destination=destination,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
            write_disposition="WRITE_TRUNCATE",
        )

        sql = f"""
            SELECT * EXCEPT (submission_date)
            FROM iprospect.adspend_raw_v1
            WHERE submission_date = '{args.date}'
            AND `date` = '{active_date}'
        """
        job = client.query(sql, job_config=job_config)
        print(f"Running job {job.job_id}")
        job.result()
        print(f"Updated ad spend data for {active_date}")

    with ThreadPool(args.parallelism) as pool:
        pool.map(import_data, date_list)


if __name__ == "__main__":
    main()
