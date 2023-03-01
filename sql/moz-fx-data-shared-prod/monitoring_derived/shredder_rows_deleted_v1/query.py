"""Monitor the number of rows deleted by shredder."""
import datetime
from argparse import ArgumentParser
from multiprocessing.pool import ThreadPool
from pathlib import Path

import yaml
from google.cloud import bigquery

from bigquery_etl.util import standard_args

PROJECT_IDS_QUERY = """
SELECT DISTINCT
  SPLIT(job_id, ":")[OFFSET(0)] AS project_id,
FROM
  `moz-fx-data-shredder.shredder_state.shredder_state`
WHERE
  DATE(job_created) BETWEEN @end_date - 1 AND @end_date
"""

JOB_INFO_QUERY = """
WITH shredder AS (
  SELECT DISTINCT
    job_id AS full_job_id,
    SPLIT(job_id, ":")[OFFSET(0)] AS project_id,
    SPLIT(job_id, ".")[OFFSET(1)] AS job_id,
  FROM
    `moz-fx-data-shredder.shredder_state.shredder_state`
  WHERE
    DATE(job_created) BETWEEN @end_date - 1 AND @end_date
),
successful_jobs AS (
  -- https://cloud.google.com/bigquery/docs/information-schema-jobs
  SELECT
    *
  FROM
    `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE
    DATE(creation_time) BETWEEN @end_date - 1 AND @end_date
    AND DATE(end_time) = @end_date
    AND state = "DONE"
    AND error_result IS NULL
)
SELECT
  successful_jobs.end_time,
  shredder.full_job_id AS job_id,
  successful_jobs.destination_table.project_id,
  successful_jobs.destination_table.dataset_id,
  successful_jobs.destination_table.table_id,
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


def get_job_info(
    client: bigquery.Client, project_id: str, end_date: datetime.date
) -> list[dict]:
    query = client.query(
        JOB_INFO_QUERY.format(project_id=project_id),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        ),
    )
    return list(map(dict, query.result()))


def add_job_metadata(client: bigquery.Client, job: dict) -> dict:
    if job["deleted_row_count"] is None:
        table = f'{job["project_id"]}.{job["dataset_id"]}.{job["table_id"]}'
        before = client.get_table(f'{table}@{job["start_time_millis"]}').num_rows
        after = client.get_table(f'{table}@{job["end_time_millis"]}').num_rows
        job["deleted_row_count"] = count = before - after
        if count < 0:
            raise ValueError(f"deleted_row_count must be >= 0, but got: {count}")
    # move partition id to separate column
    if "$" in job["table_id"]:
        job["table_id"], _, job["partition_id"] = job["table_id"].partition("$")
    job["end_time"] = job["end_time"].isoformat()
    return job


def main():
    args = parser.parse_args()

    client = bigquery.Client()

    query = client.query(
        PROJECT_IDS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("end_date", "DATE", args.end_date),
            ]
        ),
    )
    project_ids = [row["project_id"] for row in query.result()]

    with ThreadPool(args.parallelism) as pool:
        jobs = sum(
            pool.starmap(
                get_job_info,
                ((client, project_id, args.end_date) for project_id in project_ids),
                chunksize=1,
            ),
            start=[],
        )
        json_rows = list(
            pool.starmap(
                add_job_metadata, ((client, job) for job in jobs), chunksize=1
            ),
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
