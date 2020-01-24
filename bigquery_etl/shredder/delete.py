"""Delete user data from long term storage."""

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from functools import partial
from textwrap import dedent
import asyncio
import logging
import warnings

from google.cloud import bigquery

from ..util.client_queue import ClientQueue
from ..util.temp_table import get_temporary_table
from ..util.table_filter import add_table_filter_arguments, get_table_filter
from ..util.sql_table_id import sql_table_id
from .config import ClusterCondition, DELETE_TARGETS


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
parser.add_argument(
    "--ignore-cluster-conditions",
    "--ignore_cluster_conditions",
    action="store_true",
    help="Ignore cluster conditions; Used to process main_v4 using DELETE queries; "
    "Should be combined with --billing-projects that use on-demand pricing",
)
add_table_filter_arguments(parser)

WHERE_CLAUSE = """
WHERE
  {target.field} IN (
    SELECT DISTINCT
      {source.field}
    FROM
      `{source.sql_table_id}`
    WHERE
      {source_condition}
  )
  AND {partition_condition}
"""

DELETE_TEMPLATE = f"""
DELETE
  `{{target.sql_table_id}}`
{WHERE_CLAUSE.strip()}
"""

SELECT_TEMPLATE = f"""
CREATE TABLE
  `{{destination.project}}.{{destination.dataset_id}}.{{destination.table_id}}`
PARTITION BY
  {{partition_expr}}
{{clustering}}
OPTIONS (
  expiration_timestamp = '{{expiration_timestamp}}'
)
AS
SELECT
  *
FROM
  `{{target.sql_table_id}}`
{WHERE_CLAUSE.strip()}
  AND {{cluster_condition}}
"""


def record_state(client, state_table, task_id, job, dry_run, start_date, end_date):
    """Record the job for task_id in state_table."""
    if state_table is not None:
        job_id = "a job_id" if dry_run else f"{job.project}.{job.location}.{job.job_id}"
        insert_tense = "Would insert" if dry_run else "Inserting"
        logging.info(f"{insert_tense} {job_id} in {state_table} for task: {task_id}")
        if not dry_run:
            client.query(
                dedent(
                    f"""
                    INSERT INTO
                      `{state_table}`(
                        task_id,
                        job,
                        job_started,
                        start_date,
                        end_date
                      )
                    VALUES
                      (
                        "{task_id}",
                        "{job_id}",
                        TIMESTAMP "{job.started:%Y-%m-%d %H:%M:%S} UTC",
                        DATE "{start_date}",
                        DATE "{end_date}"
                      )
                    """
                ).strip()
            ).result()


def wait_for_job(
    client, state_table, states, task_id, dry_run, start_date, end_date, create_job
):
    """Get a job from state or create a new job, and wait for the job to complete."""
    job = states.get(task_id)
    if job:
        project, location, job_id = job.split(".")
        job = client.get_job(job_id, project, location)
        if job.errors:
            logging.info(f"Previous attempt failed, retrying: {task_id}")
            job = None
        elif job.ended:
            logging.info(f"Previous attempt succeeded, reusing result: {task_id}")
        else:
            logging.info(f"Previous attempt still running: {task_id}")
    if job is None:
        job = create_job(client)
        record_state(client, state_table, task_id, job, dry_run, start_date, end_date)
    if not dry_run and not job.ended:
        logging.info(f"Waiting on {job.project}.{job.location}.{job.job_id}: {task_id}")
        job.result()
    return job


def get_task_id(job_type, target, partition_id=None, cluster_condition=None):
    """Get unique task id for state tracking."""
    task_id = f"{job_type} for {target.sql_table_id}"
    if partition_id:
        task_id += f"${partition_id}"
    if cluster_condition:
        task_id += f" where {cluster_condition}"
    return task_id


async def delete_from_cluster(
    executor,
    client_q,
    dry_run,
    priority,
    states,
    state_table,
    target,
    partition_id,
    cluster_condition,
    clustering_fields,
    update_clustering,
    start_date,
    end_date,
    **template_kwargs,
):
    """Process deletion requests for a cluster condition on a partition."""
    # noqa: D202

    def create_job(client):
        if cluster_condition is None:
            query = DELETE_TEMPLATE.format(target=target, **template_kwargs)
        else:
            query = SELECT_TEMPLATE.format(
                destination=get_temporary_table(client),
                cluster_condition=cluster_condition,
                target=target,
                **template_kwargs,
            )
        run_tense = "Would run" if dry_run else "Running"
        logging.debug(f"{run_tense} query: {query}")
        return client.query(
            query, bigquery.QueryJobConfig(dry_run=dry_run, priority=priority)
        )

    job = await client_q.async_with_client(
        executor,
        partial(
            wait_for_job,
            state_table=state_table,
            states=states,
            task_id=get_task_id("delete", target, partition_id, cluster_condition),
            dry_run=dry_run,
            start_date=start_date,
            end_date=end_date,
            create_job=create_job,
        ),
    )
    if update_clustering:
        destination = sql_table_id(job.destination)
        if dry_run:
            logging.debug(f"Would update clustering on {destination}")
        else:
            table = client_q.default_client.get_table(job.destination)
            if table.clustering_fields != clustering_fields:
                logging.debug(f"Updating clustering on {destination}")
                table.clustering_fields = clustering_fields
                client_q.default_client.update_table(table, ["clustering"])
    return job


async def delete_from_partition(
    client_q,
    dry_run,
    target,
    partition_id,
    clustering,
    start_date,
    end_date,
    state_table,
    states,
    **kwargs,
):
    """Process deletion requests for a single partition of a target table."""
    client = client_q.default_client
    jobs = await asyncio.gather(
        *[
            delete_from_cluster(
                client_q=client_q,
                dry_run=dry_run,
                target=target,
                partition_id=partition_id,
                clustering=(clustering if cluster.needs_clustering else ""),
                start_date=start_date,
                end_date=end_date,
                state_table=state_table,
                states=states,
                update_clustering=cluster.needs_clustering is False,
                cluster_condition=cluster.condition,
                **kwargs,
            )
            for cluster in (target.cluster_conditions or [ClusterCondition(None, None)])
        ]
    )
    if target.cluster_conditions:
        # copy results into place and delete temp tables
        sources = [sql_table_id(job.destination) for job in jobs]
        dest = f"{target.sql_table_id}${partition_id}"
        overwrite_tense = "Would overwrite" if dry_run else "Overwriting"
        logging.info(f"{overwrite_tense} {dest} by copying {len(sources)} temp tables")
        logging.debug(f"{overwrite_tense} {dest} by copying {', '.join(sources)}")
        if not dry_run:

            def create_job(client):
                return client.copy_table(
                    sources,
                    dest,
                    bigquery.CopyJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    ),
                )

            wait_for_job(
                client=client_q.default_client,
                state_table=state_table,
                states=states,
                task_id=get_task_id(
                    job_type="copy", target=target, partition_id=partition_id
                ),
                dry_run=dry_run,
                start_date=start_date,
                end_date=end_date,
                create_job=create_job,
            )
        delete_tense = "Would delete" if dry_run else "Deleting"
        logging.info(f"{delete_tense} {len(sources)} temp tables")
        for table in sources:
            logging.debug(f"{delete_tense} {table}")
            if not dry_run:
                client.delete_table(table)
    return sum(
        job.total_bytes_processed
        for job in jobs
        if job.total_bytes_processed is not None
    )


def get_partition_expr(table):
    """Get the SQL expression to use for a table's partitioning field."""
    for field in table.schema:
        if field.name != table.time_partitioning.field:
            continue
        if field.field_type == "TIMESTAMP":
            return f"DATE({field.name})"
        return field.name


def list_partitions(client, target):
    """List the partition ids and corresponding dates in a table."""
    return [
        (partition_id, datetime.strptime(partition_id, "%Y%m%d").date())
        for row in client.query(
            dedent(
                f"""
                SELECT
                  partition_id
                FROM
                  [{target.sql_table_id.replace('.',':',1)}$__PARTITIONS_SUMMARY__]
                """
            ).strip(),
            bigquery.QueryJobConfig(use_legacy_sql=True),
        ).result()
        for partition_id in [row["partition_id"]]
    ]


async def delete_from_table(
    client_q, target, dry_run, end_date, max_single_dml_bytes, **kwargs
):
    """Process deletion requests for a single target table."""
    client = client_q.default_client
    table = client.get_table(target.sql_table_id)
    clustering = f"CLUSTER BY {', '.join(table.clustering_fields)}"
    partition_expr = get_partition_expr(table)
    bytes_deleted = 0
    bytes_processed = sum(
        await asyncio.gather(
            *[
                delete_from_partition(
                    client_q=client_q,
                    clustering_fields=table.clustering_fields,
                    clustering=clustering,
                    dry_run=dry_run,
                    partition_condition=(
                        f"{partition_expr} <= '{end_date}'"
                        if partition_date is None
                        else f"{partition_expr} = '{partition_date}'"
                    ),
                    partition_expr=partition_expr,
                    partition_id=partition_id,
                    target=target,
                    end_date=end_date,
                    **kwargs,
                )
                for partition_id, partition_date in (
                    list_partitions(client=client, target=target)
                    if table.num_bytes > max_single_dml_bytes
                    or target.cluster_conditions
                    else [(None, None)]
                )
                if partition_date is None or partition_date < end_date
            ]
        )
    )
    if dry_run:
        logging.info(f"Would scan {bytes_processed} bytes from {target.table}")
    else:
        bytes_deleted = (
            table.num_bytes - client.get_table(target.sql_table_id).num_bytes
        )
        logging.info(
            f"Scanned {bytes_processed} bytes and "
            f"deleted {bytes_deleted} from {target.table}"
        )
    return bytes_processed, bytes_deleted


async def main():
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
    # expire in 15 days because this script may take up to 14 days
    expiration_date = datetime.utcnow().date() + timedelta(days=15)
    expiration_timestamp = f"{expiration_date} 00:00:00 UTC"
    client_q = ClientQueue(args.billing_projects, args.parallelism)
    states = {}
    if args.state_table:
        client_q.default_client.query(
            dedent(
                f"""
                CREATE TABLE IF NOT EXISTS
                  `{args.state_table}`(
                    task_id STRING,
                    job STRING,
                    job_started TIMESTAMP,
                    start_date DATE,
                    end_date DATE
                  )
                """
            ).strip()
        )
        states = {
            row["task_id"]: row["job"]
            for row in client_q.default_client.query(
                dedent(
                    f"""
                    SELECT
                      task_id,
                      job,
                    FROM
                      `{args.state_table}`
                    WHERE
                      start_date = '{args.start_date}'
                      AND end_date = '{args.end_date}'
                    ORDER BY
                      job_started
                    """
                ).strip()
            ).result()
        }
    with ThreadPoolExecutor(max_workers=args.parallelism) as executor:
        results = await asyncio.gather(
            *[
                delete_from_table(
                    client_q=client_q,
                    executor=executor,
                    target=replace(
                        target,
                        project=args.target_project or target.project,
                        cluster_conditions=(
                            None
                            if args.ignore_cluster_conditions
                            else target.cluster_conditions
                        ),
                    ),
                    source=replace(
                        source, project=args.source_project or source.project
                    ),
                    source_condition=source_condition,
                    dry_run=args.dry_run,
                    priority=args.priority,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_single_dml_bytes=args.max_single_dml_bytes,
                    state_table=args.state_table,
                    states=states,
                    expiration_timestamp=expiration_timestamp,
                )
                for target, source in DELETE_TARGETS.items()
                if table_filter(target.table)
            ]
        )
    if not results:
        logging.error("No tables selected")
        parser.exit(1)
    bytes_processed, bytes_deleted = map(sum, zip(*results))
    if args.dry_run:
        logging.info(f"Would scan {bytes_processed} in total")
    else:
        logging.info(f"Scanned {bytes_processed} and deleted {bytes_deleted} in total")


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    asyncio.run(main())
