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
        }
        res.append(bq_result)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("report_uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("label", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("probability", "FLOAT"),
        ],
        write_disposition="WRITE_APPEND",
    )

    predictions_table = f"{bq_dataset_id}.bugbug_predictions"

    job = client.load_table_from_json(
        res,
        predictions_table,
        job_config=job_config,
    )

    logging.info("Writing to `bugbug_predictions` table")

    try:
        job.result()
    except Exception as e:
        print(f"ERROR: {e}")
        if job.errors:
            for error in job.errors:
                logging.error(error)

    table = client.get_table(predictions_table)
    logging.info(f"Loaded {len(res)} rows into {table}")


def save_translations(client, bq_dataset_id, results):
    res = []
    for uuid, result in results.items():
        if not result["status"]:
            bq_result = {
                "report_uuid": uuid,
                "translated_text": result["translated_text"],
                "language_code": result["language_code"],
            }
            res.append(bq_result)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("report_uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("translated_text", "STRING"),
            bigquery.SchemaField("language_code", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )

    translations_table = f"{bq_dataset_id}.translations"

    job = client.load_table_from_json(
        res,
        translations_table,
        job_config=job_config,
    )

    logging.info("Writing to `translations` table")

    try:
        job.result()
    except Exception as e:
        print(f"ERROR: {e}")
        if job.errors:
            for error in job.errors:
                logging.error(error)

    table = client.get_table(translations_table)
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


def get_reports_since_last_run(client, last_run_time, bq_dataset_id):
    query = f"""
            SELECT
                reports.uuid,
                reports.comments as body,
                COALESCE(reports.url, reports.uuid) as title,
                translations.translated_text
            FROM
            `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports_live`
            AS reports
            LEFT JOIN `{bq_dataset_id}.translations` AS translations
            ON reports.uuid = translations.report_uuid
            WHERE reported_at >= "{last_run_time}" AND comments != ""
            ORDER BY reported_at
        """
    query_job = client.query(query)
    return list(query_job.result())


def get_missed_reports(client, last_run_time, bq_dataset_id):
    query = f"""
            SELECT
                reports.uuid,
                reports.comments as body,
                COALESCE(reports.url, reports.uuid) as title,
                translations.translated_text
            FROM
            `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports_live`
            AS reports
            LEFT JOIN `{bq_dataset_id}.bugbug_predictions` AS predictions
            ON reports.uuid = predictions.report_uuid
            LEFT JOIN `{bq_dataset_id}.translations` AS translations
            ON reports.uuid = translations.report_uuid
            WHERE predictions.report_uuid IS NULL AND reported_at < "{last_run_time}"
            AND reports.comments != ""
            ORDER BY reports.reported_at
        """
    query_job = client.query(query)
    return list(query_job.result())


def translate_by_uuid(client, uuids, bq_dataset_id):
    uuids_sql = ", ".join(f"'{uuid}'" for uuid in uuids)

    query = f"""
            WITH reports AS (
                SELECT uuid, comments as text_content
                FROM
                `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports_live`
                WHERE uuid IN ({uuids_sql})
            )
            SELECT
                uuid,
                STRING(
                    ml_translate_result.translations[0].detected_language_code
                ) AS language_code,
                STRING(
                    ml_translate_result.translations[0].translated_text
                ) AS translated_text,
                ml_translate_status as status
            FROM
            ML.TRANSLATE(
                MODEL `{bq_dataset_id}.translation`,
                TABLE reports,
                STRUCT(
                  'translate_text' AS translate_mode,
                  'en' AS target_language_code
                )
            );
    """
    query_job = client.query(query)
    return list(query_job.result())


def translate_reports(client, reports, bq_dataset_id):
    result = {}
    # Only translate reports that weren't translated
    uuids_to_translate = [d["uuid"] for d in reports if not d["translated_text"]]

    if uuids_to_translate:
        translation_results = translate_by_uuid(
            client, uuids_to_translate, bq_dataset_id
        )
        result = {
            result["uuid"]: {field: value for field, value in result.items()}
            for result in translation_results
            if not result["status"]
        }

    return result


def deduplicate_reports(reports):
    seen = set()
    return [
        {field: value for field, value in report.items()}
        for report in reports
        if report["uuid"] not in seen and not seen.add(report["uuid"])
    ]


def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i : i + size]


@click.command()
@click.option("--bq_project_id", help="BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="BigQuery dataset id", required=True)
def main(bq_project_id, bq_dataset_id):
    client = bigquery.Client(project=bq_project_id)

    # Get datetime of the last classification run
    last_run_time = get_last_classification_datetime(client, bq_dataset_id)

    # Get reports that were filed since last classification run
    # and have non-empty descriptions as well as reports that were missed
    new_reports = get_reports_since_last_run(client, last_run_time, bq_dataset_id)
    missed_reports = get_missed_reports(client, last_run_time, bq_dataset_id)

    combined = missed_reports + new_reports

    deduplicated_combined = deduplicate_reports(combined)

    translated = translate_reports(client, deduplicated_combined, bq_dataset_id)

    if translated:
        save_translations(client, bq_dataset_id, translated)

    for report in deduplicated_combined:
        if report["uuid"] in translated:
            report["translated_text"] = translated[report["uuid"]]["translated_text"]

    if not deduplicated_combined:
        logging.info(
            f"No new reports with filled descriptions were found since {last_run_time}"
        )
        return

    result_count = 0

    try:
        for chunk in chunk_list(deduplicated_combined, 20):
            objects_dict = {
                row["uuid"]: {
                    "uuid": row["uuid"],
                    "title": row["title"],
                    "body": row.get("translated_text", row["body"]),
                }
                for row in chunk
            }
            logging.info("Getting classification results from bugbug.")
            result = get_reports_classification(
                "invalidcompatibilityreport", objects_dict
            )

            if result:
                result_count += len(result)
                logging.info("Saving classification results to BQ.")
                add_classification_results(client, bq_dataset_id, result)

            record_classification_run(client, bq_dataset_id, True, len(result))

    except Exception as e:
        logging.error(e)
        record_classification_run(client, bq_dataset_id, False, 0)
        raise

    finally:
        logging.info(f"Total processed reports count: {result_count}")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
