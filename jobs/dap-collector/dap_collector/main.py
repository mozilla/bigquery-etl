import asyncio
import click
import datetime
import json
import math
import os
import re
import subprocess
import time
import typing

from google.cloud import bigquery
import requests

LEADER = "https://dap-07-1.api.divviup.org"
CMD = f"./collect --task-id {{task_id}} --leader {LEADER} --vdaf {{vdaf}} {{vdaf_args}} --authorization-bearer-token {{auth_token}} --batch-interval-start {{timestamp}} --batch-interval-duration {{duration}} --hpke-config {{hpke_config}} --hpke-private-key {{hpke_private_key}}"
INTERVAL_LENGTH = 300


def read_tasks(task_config_url):
    """Read task configuration from Google Cloud bucket."""

    resp = requests.get(task_config_url)
    tasks = resp.json()
    return tasks


def toh(timestamp):
    """Turn a timestamp into a datetime object which prints human readably."""
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


async def collect_once(task, timestamp, duration, hpke_private_key, auth_token):
    """Runs collection for a single time interval.

    This uses the Janus collect binary. The result is formatted to fit the BQ table.
    """
    collection_time = str(datetime.datetime.now(datetime.timezone.utc).timestamp())
    print(f"{collection_time} Collecting {toh(timestamp)} - {toh(timestamp+duration)}")

    # Prepare output
    res = {}
    res["metric_type"] = task["metric_type"]
    res["task_id"] = task["task_id"]

    res["collection_time"] = collection_time
    res["slot_start"] = timestamp

    # Convert VDAF description to string for command line use
    vdaf_args = ""
    for k, v in task["vdaf_args_structured"].items():
        vdaf_args += f" --{k} {v}"

    cmd = CMD.format(
        timestamp=timestamp,
        duration=duration,
        hpke_private_key=hpke_private_key,
        auth_token=auth_token,
        task_id=task["task_id"],
        vdaf=task["vdaf"],
        vdaf_args=vdaf_args,
        hpke_config=task["hpke_config"],
    )

    # How long an individual collection can take before it is killed.
    timeout = 100
    start_counter = time.perf_counter()
    try:
        proc = await asyncio.wait_for(
            asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            ),
            timeout,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        stdout = stdout.decode()
        stderr = stderr.decode()
    except asyncio.exceptions.TimeoutError:
        res["collection_duration"] = time.perf_counter() - start_counter
        res["error"] = f"TIMEOUT"
        return res
    res["collection_duration"] = time.perf_counter() - start_counter

    # Parse the output of the collect binary
    if proc.returncode == 1:
        if (
            stderr
            == "Error: HTTP response status 400 Bad Request - The number of reports included in the batch is invalid.\n"
        ):
            res["error"] = "BATCH TOO SMALL"
        else:
            res["error"] = f"UNHANDLED ERROR: {stderr }"
    else:
        for line in stdout.splitlines():
            if line.startswith("Aggregation result:"):
                if task["vdaf"] in ["countvec", "sumvec"]:
                    entries = line[21:-1]
                    entries = list(map(int, entries.split(",")))
                    res["value"] = entries
                elif task["vdaf"] == "sum":
                    s = int(line[20:])
                    res["value"] = [s]
                else:
                    raise RuntimeError(f"Unknown VDAF: {task['vdaf']}")
            elif line.startswith("Number of reports:"):
                res["report_count"] = int(line.split()[-1].strip())
            elif (
                line.startswith("Interval start:")
                or line.startswith("Interval end:")
                or line.startswith("Interval length:")
            ):
                # irrelevant since we are using time interval queries
                continue
            else:
                print(f"UNHANDLED OUTPUT LINE: {line}")
                raise NotImplementedError

    return res


async def process_queue(q: asyncio.Queue, results: list):
    """Worker for parallelism. Processes items from the qeueu until it is empty."""
    while not q.empty():
        job = q.get_nowait()
        res = await collect_once(*job)
        results.append(res)


async def collect_many(
    task, time_from, time_until, interval_length, hpke_private_key, auth_token
):
    """Collects data for a given time interval.

    Creates a configurable amount of workers which process jobs from a queue
    for parallelism.
    """
    time_from = int(time_from.timestamp())
    time_until = int(time_until.timestamp())
    start = math.ceil(time_from // interval_length) * interval_length
    jobs = asyncio.Queue(288)
    results = []
    while start + interval_length <= time_until:
        await jobs.put((task, start, interval_length, hpke_private_key, auth_token))
        start += interval_length
    workers = []
    for _ in range(10):
        workers.append(process_queue(jobs, results))
    await asyncio.gather(*workers)

    return results


async def collect_task(task, auth_token, hpke_private_key, date):
    """Collects data for the given task and the given day."""
    start = datetime.datetime.fromisoformat(date)
    start = start.replace(tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(days=1)

    results = await collect_many(
        task, start, end, INTERVAL_LENGTH, hpke_private_key, auth_token
    )

    return results


def ensure_table(bqclient, table_id):
    """Checks if the table exists in BQ and creates it otherwise.
    Fails if the table exists but has the wrong schema.
    """
    schema = [
        bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("collection_duration", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("report_count", "INTEGER"),
        bigquery.SchemaField("error", "STRING"),
        bigquery.SchemaField("value", "INTEGER", mode="REPEATED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    print(f"Making sure the table {table_id} exists.")
    table = bqclient.create_table(table, exists_ok=True)


def store_data(results, bqclient, table_id):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    insert_res = bqclient.insert_rows_json(table=table_id, json_rows=results)
    if len(insert_res) != 0:
        print(insert_res)
        assert len(insert_res) == 0


@click.command()
@click.option("--project", help="GCP project id", required=True)
@click.option(
    "--table-id",
    help="The aggregated DAP measurements will be stored in this table.",
    required=True,
)
@click.option(
    "--auth-token",
    help="HTTP bearer token to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke-private-key",
    help="The private key used to decrypt shares from the leader and helper.",
    required=True,
)
@click.option(
    "--date",
    help="Date at which the backfill will start, going backwards (YYYY-MM-DD)",
    required=True,
)
@click.option(
    "--task-config-url",
    help="URL where a JSON definition of the tasks to be collected can be found.",
    required=True,
)
def main(project, table_id, auth_token, hpke_private_key, date, task_config_url):
    table_id = project + "." + table_id
    bqclient = bigquery.Client(project=project)
    ensure_table(bqclient, table_id)
    for task in read_tasks(task_config_url):
        print(f"Now processing task: {task['task_id']}")
        results = asyncio.run(collect_task(task, auth_token, hpke_private_key, date))
        store_data(results, bqclient, table_id)


if __name__ == "__main__":
    main()
