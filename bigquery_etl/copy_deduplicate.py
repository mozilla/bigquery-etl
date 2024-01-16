"""
Copy a day's data from live to stable ping tables, deduplicating on document_id.

By default, the script will process all tables in datasets named
like *_live, copying data into table of the same name in *_stable
datasets. The script can be configured to exclude a list of tables
or to process only a specific list of tables.
"""

import json
import logging
from datetime import datetime, timedelta
from functools import partial
from itertools import groupby
from multiprocessing.pool import ThreadPool

import click
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.cli.utils import table_matches_patterns
from bigquery_etl.util.bigquery_id import sql_table_id
from bigquery_etl.util.client_queue import ClientQueue
from bigquery_etl.util.common import TempDatasetReference

from .cli.utils import parallelism_option, project_id_option

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
    -- Retain only the first seen documents for each ID, according to timestamp.
    USING (document_id, submission_timestamp)
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


@click.command(
    "copy_deduplicate",
    help="Copy a day's data from live to stable ping tables, deduplicating on document_id",
)
@project_id_option("moz-fx-data-shar-nonprod-efed")
@click.option(
    "--dates",
    "--date",
    multiple=True,
    required=True,
    type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    help="One or more days of data to copy, in format 2019-01-01",
)
@parallelism_option()
@click.option(
    "--dry_run",
    "--dry-run",
    is_flag=True,
    help="Do not make changes, only log actions that would be taken",
)
@click.option(
    "--log-level",
    "--log_level",
    help="Log level.",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)
@click.option(
    "--priority",
    default=bigquery.QueryPriority.INTERACTIVE,
    type=click.Choice(
        [bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
        case_sensitive=False,
    ),
    help="Priority for BigQuery query jobs; BATCH priority may significantly slow "
    "down queries if reserved slots are not enabled for the billing project; "
    "INTERACTIVE priority is limited to 100 concurrent queries per project",
)
@click.option(
    "--temp-dataset",
    "--temp_dataset",
    "--temporary-dataset",
    "--temporary_dataset",
    default="moz-fx-data-shared-prod.tmp",
    type=TempDatasetReference.from_string,
    help="Dataset where intermediate query results will be temporarily stored, "
    "formatted as PROJECT_ID.DATASET_ID",
)
@click.option(
    "--slices",
    type=int,
    default=1,
    help=(
        "Number of queries to split deduplicate over, each handling an equal-size time "
        "slice of the date; avoids memory overflow at the cost of less effective "
        "clustering; recommended only for tables failing due to memory overflow"
    ),
)
@click.option(
    "--hourly",
    is_flag=True,
    help="Deduplicate one hour at a time; equivalent to --slices=24",
)
@click.option(
    "--preceding_days",
    "--preceding-days",
    type=int,
    default=0,
    help="Number of days preceding --date that should be used to filter out duplicates",
)
@click.option(
    "--num_retries",
    "--num-retries",
    type=int,
    default=2,
    help="Number of times to retry each slice in case of query error",
)
@click.option(
    "--billing-projects",
    "--billing_projects",
    "--billing-project",
    "--billing_project",
    "-p",
    multiple=True,
    default=[None],
    help="One or more billing projects over which bigquery jobs should be "
    "distributed",
)
@click.option(
    "--except",
    "-x",
    "exclude",
    multiple=True,
    help="Process all tables except for the given tables",
)
@click.option(
    "--only",
    "-o",
    multiple=True,
    help="Process only the given tables",
)
def copy_deduplicate(
    project_id,
    dates,
    parallelism,
    dry_run,
    log_level,
    priority,
    temp_dataset,
    slices,
    hourly,
    preceding_days,
    num_retries,
    billing_projects,
    exclude,
    only,
):
    """Copy a day's data from live to stable ping tables, dedup on document_id."""
    # create a queue for balancing load across projects
    client_q = ClientQueue(billing_projects, parallelism)

    if hourly:
        slices = 24

    table_filter = partial(table_matches_patterns, "*", False)

    if only:
        table_filter = partial(table_matches_patterns, list(only), False)
    elif exclude:
        table_filter = partial(table_matches_patterns, list(exclude), True)

    with ThreadPool(parallelism) as pool:
        with client_q.client() as client:
            live_tables = _list_live_tables(
                client=client,
                pool=pool,
                project_id=project_id,
                only_tables=only,
                table_filter=table_filter,
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
                            dry_run,
                            slices,
                            priority,
                            preceding_days,
                            num_retries,
                            temp_dataset,
                        )
                        for live_table in live_tables
                        for date in dates
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
