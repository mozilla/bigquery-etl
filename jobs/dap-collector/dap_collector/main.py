from google.cloud import bigquery
import click
import datetime
import math
import os
import re
import subprocess
import time
import typing

HPKE_CONFIG = "MgAgAAEAAQAgjsCwHpkLTLxj1Y01O8FiidMmU1gGRd2Vnwv8ctXLRH8"
CMD = f"AUTH_TOKEN={{auth_token}} HPKE_PRIVATE_KEY={{hpke_private_key}} ./collect --task-id {{task_id}} --leader https://janus-leader-test.dev.mozaws.net/ --vdaf {{vdaf}} {{vdaf_args}} --hpke-config {HPKE_CONFIG} --batch-interval-start {{timestamp}} --batch-interval-duration {{duration}}"
INTLEN = 300
TASKS = [
    {
        "task_id": "DSZGMFh26hBYXNaKvhL_N4AHA3P5lDn19on1vFPBxJM",
        "vdaf": "countvec",
        "vdaf_args_structured": {"length": 1024},
        "vdaf_args": "--length 1024",
        "metric_type": "vector",
    },
    {
        "task_id": "QjMD4n8l_MHBoLrbCfLTFi8hC264fC59SKHPviPF0q8",
        "vdaf": "sum",
        "vdaf_args": "--bits 2",
        "metric_type": "integer",
    },
]


def toh(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def collect_once(task, timestamp, duration, hpke_private_key, auth_token):
    cmd = CMD.format(
        timestamp=timestamp,
        duration=duration,
        hpke_private_key=hpke_private_key,
        auth_token=auth_token,
        task_id=task["task_id"],
        vdaf=task["vdaf"],
        vdaf_args=task["vdaf_args"],
    )
    timeout = 10
    res = [None, None]
    try:
        completed_proc = subprocess.run(
            cmd, shell=True, capture_output=True, timeout=timeout
        )
        returncodes = [
            0,  # all good
            1,  # not enough reports?
        ]
        if completed_proc.returncode not in returncodes:
            print(cmd)
            print(f"Calling collector failed with {completed_proc.returncode}")
            print(completed_proc.stderr)
            print(completed_proc.stdout)
            raise RuntimeError(
                f"Calling collector failed with {completed_proc.returncode}"
            )
        output = completed_proc.stdout.decode(encoding="utf-8")
        for line in output.splitlines():
            if line.startswith("Aggregation result:"):
                if task["vdaf"] == "countvec":
                    entries = line[21:-1]
                    entries = list(map(int, entries.split(",")))
                    res[0] = entries
                elif task["vdaf"] == "sum":
                    s = int(line[20:])
                    res[0] = s
                else:
                    raise NotImplemented
            if line.startswith("Number of reports:"):
                res[1] = int(line.split()[-1].strip())
            if "ERROR" in line:
                if re.search(
                    "number of reports included in the batch is invalid", line
                ):
                    res = "BATCH TOO SMALL"
                else:
                    # some other error
                    res = line
    except subprocess.TimeoutExpired:
        res = f"TIMEOUT"

    return res


def collect_many(
    task, time_from, time_until, interval_length, hpke_private_key, auth_token
):
    """Collects data for a given time interval."""
    time_from = int(time_from.timestamp())
    time_until = int(time_until.timestamp())
    start = math.ceil(time_from // interval_length) * interval_length
    results = {}
    while start + interval_length <= time_until:
        print(f"Collecting {toh(start)} - {toh(start+interval_length)}")
        results[toh(start)] = collect_once(
            task, start, interval_length, hpke_private_key, auth_token
        )
        start += interval_length
    return results


def collect_task(task, auth_token, hpke_private_key):
    """Collects data for the given task and the previous day."""
    start = datetime.datetime.now(datetime.timezone.utc)
    start = start.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - datetime.timedelta(days=1)

    end = datetime.datetime.now(datetime.timezone.utc)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0)

    results = collect_many(task, start, end, INTLEN, hpke_private_key, auth_token)

    return results


def ensure_table(bqclient, table_id):
    schema = [
        bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("report_count", "INTEGER"),
        bigquery.SchemaField("error", "STRING"),
        bigquery.SchemaField("values", "INTEGER", mode="REPEATED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    print(f"Making sure the table {table_id} exists.")
    table = bqclient.create_table(table, exists_ok=True)


def store_data(task, data, bqclient, table_id):
    rows = []
    for ts, retrieved in data.items():
        row = {
            "slot_start": str(ts),
            "collection_time": str(
                datetime.datetime.now(datetime.timezone.utc).timestamp()
            ),
            "task_id": task["task_id"],
            "metric_type": task["metric_type"],
        }
        if isinstance(retrieved, typing.List):
            n_reports = retrieved[1]
            row["report_count"] = n_reports
            if task["vdaf"] == "countvec":
                row["value"] = retrieved[0]
            elif task["vdaf"] == "sum":
                row["value"] = [retrieved[0]]
            else:
                raise NotImplemented
        else:
            row["error"] = retrieved

        rows.append(row)

    print("Inserting data into BQ.")
    insert_res = bqclient.insert_rows_json(table=table_id, json_rows=rows)

    # cheap error handling
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
def main(project, table_id, auth_token, hpke_private_key):
    table_id = project + "." + table_id
    bqclient = bigquery.Client(project=project)
    ensure_table(bqclient, table_id)
    for task in TASKS:
        print(f"Now processing task: {task['task_id']}")
        results = collect_task(task, auth_token, hpke_private_key)
        store_data(task, results, bqclient, table_id)


if __name__ == "__main__":
    main()
