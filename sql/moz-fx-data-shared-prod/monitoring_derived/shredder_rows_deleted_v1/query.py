"""Monitor the number of rows deleted by shredder."""
import datetime
from argparse import ArgumentParser
from pathlib import Path
from multiprocessing.pool import ThreadPool

import yaml
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.util import standard_args

JOB_INFO_QUERY = """
WITH shredder AS (
  SELECT DISTINCT
    job_id AS full_job_id,
    SPLIT(job_id, ":")[OFFSET(0)] AS project_id,
    SPLIT(job_id, ".")[OFFSET(1)] AS job_id,
  FROM
    `moz-fx-data-shredder.shredder_state.shredder_state`
),
jobs AS (
  -- https://cloud.google.com/bigquery/docs/information-schema-jobs
  SELECT
    *
  FROM
    `moz-fx-data-shredder.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-bq-batch-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
),
successful_jobs AS (
  SELECT
    *,
  FROM
    jobs
  WHERE
    DATE(creation_time) BETWEEN @end_date - 1 AND @end_date
    AND DATE(end_time) = @end_date
    AND state = "DONE"
    AND error_result IS NULL
)
SELECT
  successful_jobs.end_time,
  shredder.full_job_id AS job_id,
  successful_jobs.destination_table.*,
  successful_jobs.dml_statistics.deleted_row_count,
  UNIX_MILLIS(successful_jobs.start_time) AS start_time_millis,
  UNIX_MILLIS(successful_jobs.end_time) AS end_time_millis,
FROM
  shredder
JOIN
  successful_jobs
USING
  (project_id, job_id)
"""

parser = ArgumentParser()
parser.add_argument(
    "--end_date",
    "--end-date",
    type=datetime.date.fromisoformat,
    help="",
)
parser.add_argument(
    "--destination_table",
    "--destination-table",
    help="Table where results will be written",
)
standard_args.add_parallelism(parser)


def add_job_metdata(client, job):
    if job["deleted_row_count"] is None:
        table = f'{job["project_id"]}.{job["dataset_id"]}.{job["table_id"]}'
        try:
            before = client.get_table(f'{table}@{job["start_time_millis"]}').num_rows
            after = client.get_table(f'{table}@{job["end_time_millis"]}').num_rows
        except BadRequest as e:
            if "Invalid time travel timestamp" in e.message:
                return None  # row count not available
            raise
        job["deleted_row_count"] = before - after
    # move partition id to separate column
    if "$" in job["table_id"]:
        (job["table_id"], _, job["partition_id"],) = job[
            "table_id"
        ].partition("$")
    job["end_time"] = job["end_time"].isoformat()
    return job


def main():
    args = parser.parse_args()

    client = bigquery.Client()

    query = client.query(
        JOB_INFO_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("end_date", "DATE", args.end_date),
            ]
        ),
    )

    jobs = list(map(dict, query.result()))

    with ThreadPool(args.parallelism) as pool:
        json_rows = list(
            filter(
                None,
                pool.starmap(
                    add_job_metdata, ((client, job) for job in jobs), chunksize=1
                ),
            )
        )

    load_job = client.load_table_from_json(
        json_rows=json_rows,
        destination=args.destination_table,
        job_config=bigquery.LoadJobConfig(
            ignore_unknown_values=True,
            schema=yaml.safe_load((Path(__file__).parent / "schema.yaml").read_text())[
                "fields"
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(field="end_time"),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()


if __name__ == "__main__":
    main()
