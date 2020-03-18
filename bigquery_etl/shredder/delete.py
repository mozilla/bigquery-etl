"""Delete user data from long term storage."""

from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from operator import attrgetter
from textwrap import dedent
from typing import Callable, Iterable, Optional
import logging
import warnings

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ..util.bigquery_id import FULL_JOB_ID_RE, full_job_id, sql_table_id
from ..util.client_queue import ClientQueue
from ..util.table_filter import add_table_filter_arguments, get_table_filter
from ..util.exceptions import BigQueryInsertError
from .config import DELETE_TARGETS


NULL_PARTITION_ID = "__NULL__"
OUTSIDE_RANGE_PARTITION_ID = "__UNPARTITIONED__"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "-n",
    "--dry_run",
    "--dry-run",
    action="store_true",
    help="Do not make changes, only log actions that would be taken; "
    "use with --log-level=DEBUG to log query contents",
)
parser.add_argument(
    "--read_only",
    "--read-only",
    action="store_true",
    help="Use SELECT * FROM instead of DELETE with dry run queries to prevent errors "
    "due to read-only permissions being insufficient to dry run DELETE dml",
)
parser.add_argument(
    "-l",
    "--log-level",
    "--log_level",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)
parser.add_argument(
    "-P",
    "--parallelism",
    default=4,
    type=int,
    help="Maximum number of queries to execute concurrently",
)
parser.add_argument(
    "-e",
    "--end-date",
    "--end_date",
    default=datetime.utcnow().date(),
    type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    help="last date of last date of pings to delete; One day after last date of "
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
parser.add_argument(
    "-p",
    "--billing-projects",
    "--billing_projects",
    "--billing-project",
    "--billing_project",
    nargs="+",
    default=["moz-fx-data-bq-batch-prod"],
    help="One or more billing projects over which bigquery jobs should be distributed; "
    "if not specified use the bigquery-batch-prod project",
)
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
parser.add_argument(
    "--priority",
    default=bigquery.QueryPriority.INTERACTIVE,
    type=str.upper,
    choices=[bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
    help="Priority for BigQuery query jobs; BATCH priority may significantly slow "
    "down queries if reserved slots are not enabled for the billing project; "
    "INTERACTIVE priority is limited to 100 concurrent queries per project",
)
parser.add_argument(
    "--state-table",
    "--state_table",
    metavar="TABLE",
    help="Table for recording state; Used to avoid repeating deletes if interrupted; "
    "Create it if it does not exist; By default state is not recorded",
)
add_table_filter_arguments(parser)


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
    source,
    source_condition,
    target,
    **wait_for_job_kwargs,
):
    """Return callable to handle deletion requests for partitions of a target table."""
    # noqa: D202

    def create_job(client):
        query = dedent(
            f"""
            {"SELECT * FROM" if dry_run and read_only else "DELETE"}
              `{sql_table_id(target)}`
            WHERE
              {target.field} IN (
                SELECT DISTINCT
                  {source.field}
                FROM
                  `{sql_table_id(source)}`
                WHERE
                  {source_condition}
              )
              AND {partition_condition}
            """
        ).strip()
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


def list_partitions(client, table, partition_expr, end_date, max_single_dml_bytes):
    """List the relevant partitions in a table."""
    return [
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
            if table.num_bytes > max_single_dml_bytes or partition_expr is None
            else [get_partition(table, partition_expr, end_date)]
        )
        if partition is not None
    ]


@dataclass
class Task:
    """Return type for delete_from_table."""

    table: bigquery.Table
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
    client, target, dry_run, end_date, max_single_dml_bytes, **kwargs
) -> Iterable[Task]:
    """Yield tasks to handle deletion requests for a target table."""
    table = client.get_table(sql_table_id(target))
    partition_expr = get_partition_expr(table)
    for partition in list_partitions(
        client, table, partition_expr, end_date, max_single_dml_bytes
    ):
        yield Task(
            table=table,
            partition_id=partition.id,
            func=delete_from_partition(
                dry_run=dry_run,
                partition_condition=partition.condition,
                target=target,
                task_id=get_task_id(target, partition.id),
                end_date=end_date,
                **kwargs,
            ),
        )


def main():
    """Process deletion requests."""
    args = parser.parse_args()
    if args.start_date is None:
        args.start_date = args.end_date - timedelta(days=14)
    logging.root.setLevel(args.log_level)
    source_condition = (
        f"DATE(submission_timestamp) >= '{args.start_date}' "
        f"AND DATE(submission_timestamp) < '{args.end_date}'"
    )
    table_filter = get_table_filter(args)
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
                    dedent(
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
                    ).strip()
                ).result()
            )
    tasks = [
        task
        for target, source in DELETE_TARGETS.items()
        if table_filter(target.table)
        for task in delete_from_table(
            client=client,
            target=replace(target, project=args.target_project or target.project),
            source=replace(source, project=args.source_project or source.project),
            source_condition=source_condition,
            dry_run=args.dry_run,
            read_only=args.read_only,
            priority=args.priority,
            start_date=args.start_date,
            end_date=args.end_date,
            max_single_dml_bytes=args.max_single_dml_bytes,
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
        results = pool.map(
            client_q.with_client, (task.func for task in tasks), chunksize=1
        )
    bytes_processed_by_table = defaultdict(lambda: 0)
    for i, job in enumerate(results):
        bytes_processed_by_table[tasks[i].table] += job.total_bytes_processed
    bytes_processed = bytes_deleted = 0
    for table, table_bytes_processed in bytes_processed_by_table.items():
        bytes_processed += table_bytes_processed
        table_id = sql_table_id(table)
        if args.dry_run:
            logging.info(f"Would scan {table_bytes_processed} bytes from {table_id}")
        else:
            table_bytes_deleted = table.num_bytes - client.get_table(table).num_bytes
            bytes_deleted += table_bytes_deleted
            logging.info(
                f"Scanned {table_bytes_processed} bytes and "
                f"deleted {table_bytes_deleted} from {table_id}"
            )
    if args.dry_run:
        logging.info(f"Would scan {bytes_processed} in total")
    else:
        logging.info(f"Scanned {bytes_processed} and deleted {bytes_deleted} in total")


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
