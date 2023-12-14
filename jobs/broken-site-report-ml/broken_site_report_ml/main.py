import click
import datetime
import logging
import requests
import time

from google.cloud import bigquery

BUGBUG_HTTP_SERVER = "https://bugbug.herokuapp.com"
CLASSIFICATION_LABELS = {0: "valid", 1: "invalid"}


def classification_http_request(url, reports):
    reports_list = list(reports.values())
    response = requests.post(
        url, headers={"X-Api-Key": "docker-etl"}, json={"reports": reports_list}
    )

    response.raise_for_status()

    return response.json()


def get_reports_classification(model, reports, retry_count=21, retry_sleep=10):
    """Get the classification for a list of reports.

    Args:
        model: The model to use for the classification.
        reports: The dict containing reports to classify with uuid used as keys.
        retry_count: The number of times to retry the request.
        retry_sleep: The number of seconds to sleep between retries.

    Returns:
        A dictionary with the uuids as keys and classification results as values.
    """
    if len(reports) == 0:
        return {}

    url = f"{BUGBUG_HTTP_SERVER}/{model}/predict/broken_site_report/batch"

    json_response = {}

    for _ in range(retry_count):
        response = classification_http_request(url, reports)

        # Check which reports are ready
        for uuid, data in response["reports"].items():
            if not data.get("ready", True):
                continue

            # The report is ready, add it to the json_response and pop it
            # up from the current batch
            reports.pop(uuid, None)
            json_response[uuid] = data

        if len(reports) == 0:
            break
        else:
            time.sleep(retry_sleep)

    else:
        total_sleep = retry_count * retry_sleep
        msg = f"Couldn't get {len(reports)} report classifications in {total_sleep} seconds, aborting"  # noqa
        logging.error(msg)
        raise Exception(msg)

    return json_response


def add_classification_results(client, bq_dataset_id, results):
    res = []
    for uuid, result in results.items():
        bq_result = {
            "report_uuid": uuid,
            "label": CLASSIFICATION_LABELS[result["class"]],
            "created_at": datetime.datetime.utcnow().isoformat(),
            "probability": result["prob"][result["class"]],
            "is_ml": True,
        }
        res.append(bq_result)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("report_uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("label", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("probability", "FLOAT"),
            bigquery.SchemaField("is_ml", "BOOLEAN", mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )

    labels_table = f"{bq_dataset_id}.labels"

    job = client.load_table_from_json(
        res,
        labels_table,
        job_config=job_config,
    )

    logging.info("Writing to `labels` table")

    try:
        job.result()
    except Exception as e:
        print(f"ERROR: {e}")
        if job.errors:
            for error in job.errors:
                logging.error(error)

    table = client.get_table(labels_table)
    logging.info(f"Loaded {len(res)} rows into {table}")


def record_classification_run(client, bq_dataset_id, is_ok, count):
    rows_to_insert = [
        {
            "run_at": datetime.datetime.utcnow().isoformat(),
            "is_ok": is_ok,
            "report_count": count,
        },
    ]
    bugbug_runs_table = f"{bq_dataset_id}.bugbug_classification_runs"
    errors = client.insert_rows_json(bugbug_runs_table, rows_to_insert)
    if errors:
        logging.error(errors)
    else:
        logging.info("Last classification run recorded")


def get_last_classification_datetime(client, bq_dataset_id):
    query = f"""
            SELECT MAX(run_at) AS last_run_at
            FROM `{bq_dataset_id}.bugbug_classification_runs`
            WHERE is_ok = TRUE
        """
    res = client.query(query).result()
    row = list(res)[0]
    last_run_time = (
        row["last_run_at"] if row["last_run_at"] is not None else "2023-11-20T00:00:00"
    )
    return last_run_time


def get_reports_since_last_run(client, last_run_time):
    query = f"""
            SELECT
                uuid,
                comments as body,
                url as title
            FROM `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports`
            WHERE comments != "" AND reported_at > "{last_run_time}"
            ORDER BY reported_at
        """
    query_job = client.query(query)
    return list(query_job.result())


@click.command()
@click.option("--bq_project_id", help="BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="BigQuery dataset id", required=True)
def main(bq_project_id, bq_dataset_id):
    client = bigquery.Client(project=bq_project_id)

    # Get datetime of the last classification run
    last_run_time = get_last_classification_datetime(client, bq_dataset_id)

    # Only get reports that were filed since last classification run
    # and have non-empty descriptions
    rows = get_reports_since_last_run(client, last_run_time)

    if not rows:
        logging.info(
            f"No new reports with filled descriptions were found since {last_run_time}"
        )
        return

    objects_dict = {
        row["uuid"]: {field: value for field, value in row.items()} for row in rows
    }

    is_ok = True
    result_count = 0
    try:
        logging.info("Getting classification results from bugbug.")
        result = get_reports_classification("invalidcompatibilityreport", objects_dict)
        if result:
            result_count = len(result)
            logging.info("Saving classification results to BQ.")
            add_classification_results(client, bq_dataset_id, result)

    except Exception as e:
        logging.error(e)
        is_ok = False

    record_classification_run(client, bq_dataset_id, is_ok, result_count)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
