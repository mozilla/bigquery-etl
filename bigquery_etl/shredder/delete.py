"""Delete user data from long term storage."""

from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from multiprocessing.pool import ThreadPool
from operator import attrgetter
from textwrap import dedent
from typing import Callable, Iterable, Optional, Tuple
import logging
import warnings

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ..format_sql.formatter import reformat
from ..util import standard_args
from ..util.bigquery_id import FULL_JOB_ID_RE, full_job_id, sql_table_id
from ..util.client_queue import ClientQueue
from ..util.exceptions import BigQueryInsertError
from .config import (
    DeleteSource,
    DELETE_TARGETS,
    find_glean_targets,
    find_experiment_analysis_targets,
)


NULL_PARTITION_ID = "__NULL__"
OUTSIDE_RANGE_PARTITION_ID = "__UNPARTITIONED__"

parser = ArgumentParser(description=__doc__)
standard_args.add_dry_run(parser)
parser.add_argument(
    "--partition-limit",
    "--partition_limit",
    metavar="N",
    type=int,
    help="Only use the first N partitions per table; requires --dry-run",
)
parser.add_argument(
    "--read_only",
    "--read-only",
    action="store_true",
    help="Use SELECT * FROM instead of DELETE with dry run queries to prevent errors "
    "due to read-only permissions being insufficient to dry run DELETE dml",
)
standard_args.add_log_level(parser)
standard_args.add_parallelism(parser)
parser.add_argument(
    "-e",
    "--end-date",
    "--end_date",
    default=datetime.utcnow().date(),
    type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    help="last date of pings to delete; One day after last date of "
    "deletion requests to process; defaults to today in UTC",
)
parser.add_argument(
    "-s",
    "--start-date",
    "--start_date",
    type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    help="first date of deletion requests to process; DOES NOT apply to ping date; "
    "defaults to 14 days before --end-date in UTC",
)
standard_args.add_billing_projects(parser, default=["moz-fx-data-bq-batch-prod"])
parser.add_argument(
    "--source-project",
    "--source_project",
    help="override the project used for deletion request tables",
)
parser.add_argument(
    "--target-project",
    "--target_project",
    help="override the project used for target tables",
)
parser.add_argument(
    "--max-single-dml-bytes",
    "--max_single_dml_bytes",
    default=10 * 2 ** 40,
    type=int,
    help="Maximum number of bytes in a table that should be processed using a single "
    "DML query; tables above this limit will be processed using per-partition "
    "queries; this option prevents queries against large tables from exceeding the "
    "6-hour time limit; defaults to 10 TiB",
)
standard_args.add_priority(parser)
parser.add_argument(
    "--state-table",
    "--state_table",
    metavar="TABLE",
    help="Table for recording state; Used to avoid repeating deletes if interrupted; "
    "Create it if it does not exist; By default state is not recorded",
)
parser.add_argument(
    "--task-table",
    "--task_table",
    metavar="TABLE",
    help="Table for recording tasks; Used along with --state-table to determine "
    "progress; Create it if it does not exist; By default tasks are not recorded",
)
standard_args.add_table_filter(parser)


def record_state(client, task_id, job, dry_run, start_date, end_date, state_table):
    """Record the job for task_id in state_table."""
    if state_table is not None:
        job_id = "a job_id" if dry_run else full_job_id(job)
        insert_tense = "Would insert" if dry_run else "Inserting"
        logging.info(f"{insert_tense} {job_id} in {state_table} for task: {task_id}")
        if not dry_run:
            BigQueryInsertError.raise_if_present(
                errors=client.insert_rows_json(
                    state_table,
                    [
                        {
                            "task_id": task_id,
                            "job_id": job_id,
                            "job_created": job.created.isoformat(),
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        }
                    ],
                    skip_invalid_rows=False,
                )
            )


def wait_for_job(client, states, task_id, dry_run, create_job, **state_kwargs):
    """Get a job from state or create a new job, and wait for the job to complete."""
    job = None
    if task_id in states:
        job = client.get_job(**FULL_JOB_ID_RE.fullmatch(states[task_id]).groupdict())
        if job.errors:
            logging.info(f"Previous attempt failed, retrying for {task_id}")
            job = None
        elif job.ended:
            logging.info(f"Previous attempt succeeded, reusing result for {task_id}")
        else:
            logging.info(f"Previous attempt still running for {task_id}")
    if job is None:
        job = create_job(client)
        record_state(
            client=client, task_id=task_id, dry_run=dry_run, job=job, **state_kwargs
        )
    if not dry_run and not job.ended:
        logging.info(f"Waiting on {full_job_id(job)} for {task_id}")
        job.result()
    return job


def get_task_id(target, partition_id):
    """Get unique task id for state tracking."""
    task_id = sql_table_id(target)
    if partition_id:
        task_id += f"${partition_id}"
    return task_id


def delete_from_partition(
    dry_run,
    partition_condition,
    priority,
    read_only,
    sources,
    source_condition,
    target,
    **wait_for_job_kwargs,
):
    """Return callable to handle deletion requests for partitions of a target table."""
    # noqa: D202

    def create_job(client):
        field_condition = " OR ".join(
            f"""
             {field} IN (
               SELECT
                 {source.field}
               FROM
                 `{sql_table_id(source)}`
               WHERE
                 {source_condition}
             )
            """
            for field, source in zip(target.fields, sources)
        )
        query = reformat(
            f"""
            {"SELECT * FROM" if dry_run and read_only else "DELETE"}
              `{sql_table_id(target)}`
            WHERE
              ({field_condition})
              AND {partition_condition}
            """
        )
        run_tense = "Would run" if dry_run else "Running"
        logging.debug(f"{run_tense} query: {query}")
        return client.query(
            query, bigquery.QueryJobConfig(dry_run=dry_run, priority=priority)
        )

    return partial(
        wait_for_job, create_job=create_job, dry_run=dry_run, **wait_for_job_kwargs
    )


def get_partition_expr(table):
    """Get the SQL expression to use for a table's partitioning field."""
    if table.range_partitioning:
        return table.range_partitioning.field
    if table.time_partitioning:
        return f"CAST({table.time_partitioning.field} AS DATE)"


@dataclass
class Partition:
    """Return type for get_partition."""

    condition: str
    id: Optional[str] = None


def get_partition(table, partition_expr, end_date, id_=None) -> Optional[Partition]:
    """Return a Partition for id_ unless it is a date on or after end_date."""
    if id_ is None:
        if table.time_partitioning:
            return Partition(f"{partition_expr} < '{end_date}'")
        return Partition("TRUE")
    if id_ == NULL_PARTITION_ID:
        if table.time_partitioning:
            return Partition(f"{table.time_partitioning.field} IS NULL", id_)
        return Partition(f"{partition_expr} IS NULL", id_)
    if table.time_partitioning:
        date = datetime.strptime(id_, "%Y%m%d").date()
        if date < end_date:
            return Partition(f"{partition_expr} = '{date}'", id_)
        return None
    if table.range_partitioning:
        if id_ == OUTSIDE_RANGE_PARTITION_ID:
            return Partition(
                f"{partition_expr} < {table.range_partitioning.range_.start} "
                f"OR {partition_expr} >= {table.range_partitioning.range_.end}",
                id_,
            )
        if table.range_partitioning.range_.interval > 1:
            return Partition(
                f"{partition_expr} BETWEEN {id_} "
                f"AND {int(id_) + table.range_partitioning.range_.interval - 1}",
                id_,
            )
    return Partition(f"{partition_expr} = {id_}", id_)


def list_partitions(
    client, table, partition_expr, end_date, max_single_dml_bytes, partition_limit
):
    """List the relevant partitions in a table."""
    partitions = [
        partition
        for partition in (
            [
                get_partition(table, partition_expr, end_date, row["partition_id"])
                for row in client.query(
                    dedent(
                        f"""
                        SELECT
                          partition_id
                        FROM
                          [{sql_table_id(table)}$__PARTITIONS_SUMMARY__]
                        """
                    ).strip(),
                    bigquery.QueryJobConfig(use_legacy_sql=True),
                ).result()
            ]
            if table.num_bytes > max_single_dml_bytes and partition_expr is not None
            else [get_partition(table, partition_expr, end_date)]
        )
        if partition is not None
    ]
    if partition_limit:
        return sorted(partitions, key=attrgetter("id"), reverse=True)[:partition_limit]
    return partitions


@dataclass
class Task:
    """Return type for delete_from_table."""

    table: bigquery.Table
    sources: Tuple[DeleteSource]
    partition_id: Optional[str]
    func: Callable[[bigquery.Client], bigquery.QueryJob]

    @property
    def partition_sort_key(self):
        """Return a tuple to control the order in which tasks will be handled.

        When used with reverse=True, handle tasks without partition_id, then
        tasks without time_partitioning, then most recent dates first.
        """
        return (
            self.partition_id is None,
            self.table.time_partitioning is None,
            self.partition_id,
        )


def delete_from_table(
    client,
    target,
    sources,
    dry_run,
    end_date,
    max_single_dml_bytes,
    partition_limit,
    **kwargs,
) -> Iterable[Task]:
    """Yield tasks to handle deletion requests for a target table."""
    try:
        table = client.get_table(sql_table_id(target))
    except NotFound:
        logging.warning(f"Skipping {sql_table_id(target)} due to NotFound exception")
        return ()
    partition_expr = get_partition_expr(table)
    for partition in list_partitions(
        client, table, partition_expr, end_date, max_single_dml_bytes, partition_limit
    ):
        yield Task(
            table=table,
            sources=sources,
            partition_id=partition.id,
            func=delete_from_partition(
                dry_run=dry_run,
                partition_condition=partition.condition,
                target=target,
                sources=sources,
                task_id=get_task_id(target, partition.id),
                end_date=end_date,
                **kwargs,
            ),
        )


def main():
    """Process deletion requests."""
    args = parser.parse_args()
    if args.partition_limit is not None and not args.dry_run:
        parser.print_help()
        print("ERROR: --partition-limit specified without --dry-run")
    if args.start_date is None:
        args.start_date = args.end_date - timedelta(days=14)
    source_condition = (
        f"DATE(submission_timestamp) >= '{args.start_date}' "
        f"AND DATE(submission_timestamp) < '{args.end_date}'"
    )
    client_q = ClientQueue(args.billing_projects, args.parallelism)
    client = client_q.default_client
    states = {}
    if args.state_table:
        state_table_exists = False
        try:
            client.get_table(args.state_table)
            state_table_exists = True
        except NotFound:
            if not args.dry_run:
                client.create_table(
                    bigquery.Table(
                        args.state_table,
                        [
                            bigquery.SchemaField("task_id", "STRING"),
                            bigquery.SchemaField("job_id", "STRING"),
                            bigquery.SchemaField("job_created", "TIMESTAMP"),
                            bigquery.SchemaField("start_date", "DATE"),
                            bigquery.SchemaField("end_date", "DATE"),
                        ],
                    )
                )
                state_table_exists = True
        if state_table_exists:
            states = dict(
                client.query(
                    reformat(
                        f"""
                        SELECT
                          task_id,
                          job_id,
                        FROM
                          `{args.state_table}`
                        WHERE
                          start_date = '{args.start_date}'
                          AND end_date = '{args.end_date}'
                        ORDER BY
                          job_created
                        """
                    )
                ).result()
            )
    with ThreadPool(args.parallelism) as pool:
        glean_targets = find_glean_targets(pool, client)
        experiment_analysis_targets = find_experiment_analysis_targets(pool, client)
    tasks = [
        task
        for target, sources in chain(
            DELETE_TARGETS.items(),
            glean_targets.items(),
            experiment_analysis_targets.items(),
        )
        if args.table_filter(target.table)
        for task in delete_from_table(
            client=client,
            target=replace(target, project=args.target_project or target.project),
            sources=[
                replace(source, project=args.source_project or source.project)
                for source in (sources if isinstance(sources, tuple) else (sources,))
            ],
            source_condition=source_condition,
            dry_run=args.dry_run,
            read_only=args.read_only,
            priority=args.priority,
            start_date=args.start_date,
            end_date=args.end_date,
            max_single_dml_bytes=args.max_single_dml_bytes,
            partition_limit=args.partition_limit,
            state_table=args.state_table,
            states=states,
        )
    ]
    if not tasks:
        logging.error("No tables selected")
        parser.exit(1)
    # ORDER BY partition_sort_key DESC, sql_table_id ASC
    # https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts
    tasks.sort(key=lambda task: sql_table_id(task.table))
    tasks.sort(key=attrgetter("partition_sort_key"), reverse=True)
    with ThreadPool(args.parallelism) as pool:
        if args.task_table and not args.dry_run:
            # record task information
            try:
                client.get_table(args.task_table)
            except NotFound:
                table = bigquery.Table(
                    args.task_table,
                    [
                        bigquery.SchemaField("task_id", "STRING"),
                        bigquery.SchemaField("start_date", "DATE"),
                        bigquery.SchemaField("end_date", "DATE"),
                        bigquery.SchemaField("target", "STRING"),
                        bigquery.SchemaField("target_rows", "INT64"),
                        bigquery.SchemaField("target_bytes", "INT64"),
                        bigquery.SchemaField("source_bytes", "INT64"),
                    ],
                )
                table.time_partitioning = bigquery.TimePartitioning()
                client.create_table(table)
            sources = list(set(source for task in tasks for source in task.sources))
            source_bytes = {
                source: job.total_bytes_processed
                for source, job in zip(
                    sources,
                    pool.starmap(
                        client.query,
                        [
                            (
                                reformat(
                                    f"""
                                    SELECT
                                      {source.field}
                                    FROM
                                      `{sql_table_id(source)}`
                                    WHERE
                                      {source_condition}
                                    """
                                ),
                                bigquery.QueryJobConfig(dry_run=True),
                            )
                            for source in sources
                        ],
                        chunksize=1,
                    ),
                )
            }
            step = 10000  # max 10K rows per insert
            for start in range(0, len(tasks), step):
                end = start + step
                BigQueryInsertError.raise_if_present(
                    errors=client.insert_rows_json(
                        args.task_table,
                        [
                            {
                                "task_id": get_task_id(task.table, task.partition_id),
                                "start_date": args.start_date.isoformat(),
                                "end_date": args.end_date.isoformat(),
                                "target": sql_table_id(task.table),
                                "target_rows": task.table.num_rows,
                                "target_bytes": task.table.num_bytes,
                                "source_bytes": sum(
                                    map(source_bytes.get, task.sources)
                                ),
                            }
                            for task in tasks[start:end]
                        ],
                    )
                )
        results = pool.map(
            client_q.with_client, (task.func for task in tasks), chunksize=1
        )
    jobs_by_table = defaultdict(list)
    for i, job in enumerate(results):
        jobs_by_table[tasks[i].table].append(job)
    bytes_processed = rows_deleted = 0
    for table, jobs in jobs_by_table.items():
        table_bytes_processed = sum(job.total_bytes_processed for job in jobs)
        bytes_processed += table_bytes_processed
        table_id = sql_table_id(table)
        if args.dry_run:
            logging.info(f"Would scan {table_bytes_processed} bytes from {table_id}")
        else:
            table_rows_deleted = sum(job.num_dml_affected_rows for job in jobs)
            rows_deleted += table_rows_deleted
            logging.info(
                f"Scanned {table_bytes_processed} bytes and "
                f"deleted {table_rows_deleted} rows from {table_id}"
            )
    if args.dry_run:
        logging.info(f"Would scan {bytes_processed} in total")
    else:
        logging.info(
            f"Scanned {bytes_processed} and deleted {rows_deleted} rows in total"
        )


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
