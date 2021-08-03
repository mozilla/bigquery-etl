"""
Copy a day's data from live to stable ping tables, deduplicating on document_id.

By default, the script will process all tables in datasets named
like *_live, copying data into table of the same name in *_stable
datasets. The script can be configured to exclude a list of tables
or to process only a specific list of tables.
"""

import json
import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta
from itertools import groupby
from multiprocessing.pool import ThreadPool

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.util import standard_args
from bigquery_etl.util.bigquery_id import sql_table_id
from bigquery_etl.util.client_queue import ClientQueue

QUERY_TEMPLATE = """
WITH
  -- Distinct document_ids and their minimum submission_timestamp today
  -- not including document_ids that only occur on or after @end_time
  distinct_document_ids AS (
  SELECT
    document_id,
    MIN(submission_timestamp) AS submission_timestamp
  FROM
    `{live_table}`
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(
        DATE(@start_time),
        INTERVAL @num_preceding_days DAY
    )
    AND submission_timestamp < @end_time
    -- Bug 1657360
    AND 'automation' NOT IN (
      SELECT TRIM(t) FROM UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
    )
  GROUP BY
    document_id
  HAVING
    submission_timestamp >= @start_time),
  -- A single slice of a live ping table.
  base AS (
  SELECT
    *
  FROM
    `{live_table}`
  JOIN
    distinct_document_ids
  USING
    -- Retain only the first seen documents for each ID, according to timestamp.
    (document_id, submission_timestamp)
  WHERE
    submission_timestamp >= @start_time
    AND submission_timestamp < @end_time),
  --
  -- Order documents by assigning a row number.
  numbered_duplicates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY document_id) AS _n
  FROM
    base)
  --
  -- Retain only one document for each ID.
SELECT
  * EXCEPT(_n)
FROM
  numbered_duplicates
WHERE
  _n = 1
"""

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shar-nonprod-efed",
    help="ID of the project in which to find tables",
)
parser.add_argument(
    "--dates",
    "--date",
    nargs="+",
    required=True,
    type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    help="One or more days of data to copy, in format 2019-01-01",
)
standard_args.add_parallelism(parser)
standard_args.add_dry_run(parser, debug_log_queries=False)
standard_args.add_log_level(parser)
standard_args.add_priority(parser)
standard_args.add_temp_dataset(parser)
parser.add_argument(
    "--slices",
    type=int,
    default=1,
    help=(
        "Number of queries to split deduplicate over, each handling an equal-size time "
        "slice of the date; avoids memory overflow at the cost of less effective "
        "clustering; recommended only for tables failing due to memory overflow"
    ),
)
parser.add_argument(
    "--hourly",
    action="store_const",
    dest="slices",
    const=24,
    help="Deduplicate one hour at a time; equivalent to --slices=24",
)
parser.add_argument(
    "--preceding_days",
    "--preceding-days",
    type=int,
    default=0,
    help="Number of days preceding --date that should be used to filter out duplicates",
)
parser.add_argument(
    "--num_retries",
    "--num-retries",
    type=int,
    default=2,
    help="Number of times to retry each slice in case of query error",
)
standard_args.add_billing_projects(parser)
standard_args.add_table_filter(parser)


def _get_query_job_configs(
    client,
    live_table,
    date,
    dry_run,
    slices,
    priority,
    preceding_days,
    num_retries,
    temp_dataset,
):
    sql = QUERY_TEMPLATE.format(live_table=live_table)
    stable_table = f"{live_table.replace('_live.', '_stable.', 1)}${date:%Y%m%d}"
    kwargs = dict(use_legacy_sql=False, dry_run=dry_run, priority=priority)
    start_time = datetime(*date.timetuple()[:6])
    end_time = start_time + timedelta(days=1)
    if slices > 1:
        # create temporary tables with stable_table's time_partitioning and
        # clustering_fields, and a 1-day expiration
        stable_table = client.get_table(stable_table)
        ddl = "CREATE TABLE\n  `{dest}`"
        ddl += f"\nPARTITION BY\n  DATE({stable_table.time_partitioning.field})"
        if stable_table.clustering_fields:
            ddl += f"\nCLUSTER BY\n  {', '.join(stable_table.clustering_fields)}"
        ddl += (
            "\nOPTIONS"
            "\n  ("
            "\n    partition_expiration_days = CAST('inf' AS FLOAT64),"
            "\n    expiration_timestamp = "
            "TIMESTAMP_ADD(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
            "\n  )"
        )
        slice_size = (end_time - start_time) / slices
        params = [start_time + slice_size * i for i in range(slices)] + [
            end_time
        ]  # explicitly use end_time to avoid rounding errors
        return [
            (
                f"{ddl.format(dest=temp_dataset.temp_table())}\nAS\n{sql.strip()}",
                stable_table,
                bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "start_time", "TIMESTAMP", params[i]
                        ),
                        bigquery.ScalarQueryParameter(
                            "end_time", "TIMESTAMP", params[i + 1]
                        ),
                        bigquery.ScalarQueryParameter(
                            "num_preceding_days", "INT64", preceding_days
                        ),
                    ],
                    **kwargs,
                ),
                num_retries,
            )
            for i in range(slices)
        ]
    else:
        return [
            (
                sql,
                stable_table,
                bigquery.QueryJobConfig(
                    destination=stable_table,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "start_time", "TIMESTAMP", start_time
                        ),
                        bigquery.ScalarQueryParameter(
                            "end_time", "TIMESTAMP", end_time
                        ),
                        bigquery.ScalarQueryParameter(
                            "num_preceding_days", "INT64", preceding_days
                        ),
                    ],
                    **kwargs,
                ),
                num_retries,
            )
        ]


def _run_deduplication_query(client, sql, stable_table, job_config, num_retries):
    query_job = client.query(sql, job_config)
    if not query_job.dry_run:
        try:
            query_job.result()
        except BadRequest as e:
            if num_retries <= 0:
                raise
            logging.warn("Encountered bad request, retrying: ", e)
            return _run_deduplication_query(
                client, sql, stable_table, job_config, num_retries - 1
            )
    logging.info(
        f"Completed query job for {stable_table}"
        f" with params: {job_config.query_parameters}"
    )
    return stable_table, query_job


def _copy_join_parts(client, stable_table, query_jobs):
    total_bytes = sum(query.total_bytes_processed for query in query_jobs)
    if query_jobs[0].dry_run:
        api_repr = json.dumps(query_jobs[0].to_api_repr())
        if len(query_jobs) > 1:
            logging.info(f"Would process {total_bytes} bytes: [{api_repr},...]")
            logging.info(f"Would copy {len(query_jobs)} results to {stable_table}")
        else:
            logging.info(f"Would process {total_bytes} bytes: {api_repr}")
    else:
        logging.info(f"Processed {total_bytes} bytes to populate {stable_table}")
        if len(query_jobs) > 1:
            partition_id = stable_table.table_id.split("$", 1)[1]
            sources = [
                f"{sql_table_id(job.destination)}${partition_id}" for job in query_jobs
            ]
            job_config = bigquery.CopyJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            copy_job = client.copy_table(sources, stable_table, job_config=job_config)
            copy_job.result()
            logging.info(f"Copied {len(query_jobs)} results to populate {stable_table}")
            for job in query_jobs:
                client.delete_table(job.destination)
            logging.info(f"Deleted {len(query_jobs)} temporary tables")


def _contains_glob(patterns):
    return any(set("*?[").intersection(pattern) for pattern in patterns)


def _glob_dataset(pattern):
    return pattern.split(".", 1)[0]


def _list_live_tables(client, pool, project_id, only_tables, table_filter):
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [f"{project_id}.{t}" for t in only_tables if table_filter(t)]
    if only_tables and not _contains_glob(_glob_dataset(t) for t in only_tables):
        # skip list_datasets call when only_tables exists and datasets contain no globs
        live_datasets = {f"{project_id}.{_glob_dataset(t)}" for t in only_tables}
    else:
        live_datasets = [
            d.reference
            for d in client.list_datasets(project_id)
            if d.dataset_id.endswith("_live")
        ]
    return [
        sql_table_id(t)
        for tables in pool.map(client.list_tables, live_datasets)
        for t in tables
        if table_filter(f"{t.dataset_id}.{t.table_id}")
        and "beam_load_sink" not in t.table_id
    ]


def main():
    """Copy a day's data from live to stable ping tables, dedup on document_id."""
    args = parser.parse_args()

    # create a queue for balancing load across projects
    client_q = ClientQueue(args.billing_projects, args.parallelism)

    with ThreadPool(args.parallelism) as pool:
        with client_q.client() as client:
            live_tables = _list_live_tables(
                client=client,
                pool=pool,
                project_id=args.project_id,
                only_tables=getattr(args, "only_tables", None),
                table_filter=args.table_filter,
            )

            query_jobs = [
                (_run_deduplication_query, *args)
                for jobs in pool.starmap(
                    _get_query_job_configs,
                    [
                        (
                            client,  # only use one client to create temp tables
                            live_table,
                            date,
                            args.dry_run,
                            args.slices,
                            args.priority,
                            args.preceding_days,
                            args.num_retries,
                            args.temp_dataset,
                        )
                        for live_table in live_tables
                        for date in args.dates
                    ],
                )
                for args in jobs
            ]

        # preserve query_jobs order so results stay sorted by stable_table for groupby
        results = pool.starmap(client_q.with_client, query_jobs, chunksize=1)
        copy_jobs = [
            (_copy_join_parts, stable_table, [query_job for _, query_job in group])
            for stable_table, group in groupby(results, key=lambda result: result[0])
        ]
        pool.starmap(client_q.with_client, copy_jobs, chunksize=1)


if __name__ == "__main__":
    main()
