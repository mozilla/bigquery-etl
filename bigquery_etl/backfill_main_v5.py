import logging
import json
from datetime import datetime, date, timedelta
from multiprocessing.pool import ThreadPool
from pathlib import Path
from functools import partial

from google.cloud import bigquery

from bigquery_etl.util.client_queue import ClientQueue
from bigquery_etl.util.bigquery_id import FULL_JOB_ID_RE, full_job_id

STATE_FILE = Path("backfill_jobs.ndjson")
TODAY = datetime.utcnow().date()


def task(state, client):
    task_id = state["task_id"]
    table_id = state["table_id"]
    partition_date = state["partition_date"]
    job = None
    if "job_id" in state:
        job = client.get_job(**FULL_JOB_ID_RE.fullmatch(state["job_id"]).groupdict())
        if job.errors:
            logging.error(f"Previous attempt failed, retrying for {task_id}")
            job = None
        elif job.ended:
            logging.error(f"Previous attempt succeeded, reusing result for {task_id}")
        else:
            logging.error(f"Previous attempt still running for {task_id}")

    if job is None:
        query = Path(
            f"sql/moz-fx-data-shared-prod/telemetry_stable/{table_id}/query.sql"
        ).read_text()
        partition_id = partition_date.replace("-", "")
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                destination=f"moz-fx-data-shared-prod.telemetry_derived.{table_id}${partition_id}",
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "partition_value", "TIMESTAMP", partition_date
                    )
                ],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        state["job_id"] = full_job_id(job)
        with open(STATE_FILE, "a") as fp:
            fp.write(json.dumps(state) + "\n")
        logging.error(f"New attempt running for {task_id}")

    if not job.ended:
        try:
            job.result(max_results=0)
        except Exception as e:
            from traceback import print_exc

            logging.error(f"Attempt failed for {task_id}")
            print_exc()
            raise
        else:
            logging.error(f"Attempt succeeded for {task_id}")

    if not job.errors:
        state["success"] = True
        with open(STATE_FILE, "a") as fp:
            fp.write(json.dumps(state) + "\n")

    return job


def incomplete_sort_key(state):
    table_id = state["table_id"]
    days_ago = (TODAY - date.fromisoformat(state["partition_date"])).days
    maybe_running = "job_id" in state
    if maybe_running:
        is_shredder = state["job_id"].startswith("moz-fx-data-shredder")
        return 2 if is_shredder else 1, -days_ago, table_id
    return 3, days_ago, table_id


def main(
    billing_projects=[
        "moz-fx-data-bq-batch-prod",
        "moz-fx-data-bq-batch-prod",
        "moz-fx-data-shredder",
        "moz-fx-data-shredder",
        "moz-fx-data-shredder",
        "moz-fx-data-shredder",
    ],
    parallelism=6,
):
    with open(STATE_FILE) as fp:
        states = {
            state["task_id"]: state for state in (json.loads(line) for line in fp)
        }
    successful = {
        state["partition_date"]
        for state in states.values()
        if state.get("success", False)
    }

    partitions = (TODAY - date(2020, 10, 1)).days
    incomplete = sorted(
        (
            states.get(
                task_id,
                {
                    "partition_date": f"{pdate}",
                    "table_id": table_id,
                    "task_id": task_id,
                },
            )
            for table_id in (
                "saved_session_v5",
                "first_shutdown_v5",
                "main_v5",
            )
            for pdate in (TODAY - timedelta(days=i) for i in range(partitions + 1))
            for task_id in [f"{table_id}${pdate:%Y%m%d}"]
            if pdate <= date(2021, 9, 14) and f"{pdate}" not in successful
            and table_id == "main_v5"
        ),
        key=incomplete_sort_key,
    )
#   for state in incomplete[:10]:
#       print(state)
#   return

    client_q = ClientQueue(billing_projects, parallelism)
    with ThreadPool(parallelism) as pool:
        results = pool.map(
            client_q.with_client,
            (partial(task, state) for state in incomplete),
            chunksize=1,
        )


if __name__ == "__main__":
    main()
