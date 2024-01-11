"""Import data from alchemer (surveygizmo) surveys into BigQuery."""
import datetime as dt
import json
import re
from pathlib import Path

import click
import pytz
import requests
from google.cloud import bigquery


def utc_date_to_eastern_string(date_string):
    """Normalize ISO date to UTC midnight."""
    naive_dt = dt.datetime.strptime(date_string, "%Y-%m-%d")
    as_utc = pytz.utc.localize(naive_dt)
    as_eastern = as_utc.astimezone(pytz.timezone("US/Eastern"))
    return dt.datetime.strftime(as_eastern, "%Y-%m-%d+%H:%M:%S")


def date_plus_one(date_string):
    """Get the day after the current date."""
    return dt.datetime.strftime(
        dt.date.fromisoformat(date_string) + dt.timedelta(days=1), "%Y-%m-%d"
    )


def format_responses(s, date):
    """Return a nested field of responses for each user."""
    # `survey_data` is a dict with question ID as the key and question/response
    # details as the value e.g. survey_data: {'25': {'id': 25, 'type': 'RADIO',
    # 'question': 'I trust Firefox to help me with my online privacy',
    # 'section_id': 2, 'answer': 'Agree', 'answer_id': 10066, 'shown': True}}
    # See https://apihelp.alchemer.com/help/surveyresponse-returned-fields-v5#getobject

    # Note that we are omitted date_submission and date_completed because the
    # timezone is not ISO compliant. The submission date that is passed in as
    # the time parameter should suffice.
    fields = ["id", "session_id", "status", "response_time"]
    results = []
    for data in s.get("survey_data", {}).values():
        # There can be answer_id's like "123456-other"
        if data.get("answer_id") and isinstance(data["answer_id"], str):
            numeric = re.findall(r"\d+", data["answer_id"])
            if numeric:
                data["answer_id"] = int(numeric[0])
            else:
                del data["answer_id"]
        if data.get("options"):
            data["options"] = list(data["options"].values())
        if data.get("subquestions"):
            data["subquestions"] = json.dumps(data["subquestions"])
        results.append(data)

    return {
        # this is used as the partitioning field
        "submission_date": date,
        **{field: s[field] for field in fields},
        "survey_data": results,
    }


def construct_data(survey, date):
    """Construct response data."""
    return [format_responses(resp, date) for resp in survey["data"]]


def get_survey_data(survey_id, date_string, token, secret):
    """Get survey data from a survey id and date."""
    # per SurveyGizmo docs, times are assumed to be eastern
    # https://apihelp.surveygizmo.com/help/filters-v5
    # so UTC midnight must be converted to EST/EDT
    start_date = utc_date_to_eastern_string(date_string)
    end_date = utc_date_to_eastern_string(date_plus_one(date_string))

    url = (
        f"https://restapi.surveygizmo.com/v5/survey/{survey_id}/surveyresponse"
        f"?api_token={token}&api_token_secret={secret}&results_per_page=500"
        # filter for date_submitted >= start_date
        f"&filter[field][0]=date_submitted"
        f"&filter[operator][0]=>="
        f"&filter[value][0]={start_date}"
        # filter for date_submitted < end_date
        f"&filter[field][1]=date_submitted"
        f"&filter[operator][1]=<"
        f"&filter[value][1]={end_date}"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    survey = resp.json()

    # if the result set is large, we'll have to page through them to get all data
    total_pages = survey.get("total_pages")
    print(f"Found {total_pages} pages after filtering on date={date_string}")

    print("fetching page 1")
    ret = construct_data(survey, date_string)

    for page in range(2, total_pages + 1):
        print(f"fetching page {page}")
        resp = requests.get(url + f"&page={page}")
        resp.raise_for_status()
        ret = ret + construct_data(resp.json(), date_string)
    return ret


def response_schema():
    """Get the schema for the response object from disk."""
    path = Path(__file__).parent / "response.schema.json"
    return bigquery.SchemaField.from_api_repr(
        {"name": "root", "type": "RECORD", "fields": json.loads(path.read_text())}
    ).fields


def insert_to_bq(
    data, table, date, write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
):
    """Insert data into a bigquery table."""
    client = bigquery.Client()
    print(f"Inserting {len(data)} rows into bigquery")
    job_config = bigquery.LoadJobConfig(
        # We may also infer the schema by setting `autodetect=True`
        schema=response_schema(),
        schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        write_disposition=write_disposition,
        time_partitioning=bigquery.table.TimePartitioning(field="submission_date"),
    )
    partition = f"{table}${date.replace('-', '')}"
    job = client.load_table_from_json(data, partition, job_config=job_config)
    print(f"Running job {job.job_id}")
    # job.result() returns a LoadJob object if successful, or raises an exception if not
    job.result()


@click.command()
@click.option("--date", required=True)
@click.option("--survey_id", required=True)
@click.option("--api_token", required=True)
@click.option("--api_secret", required=True)
@click.option("--destination_table", required=True)
def main(date, survey_id, api_token, api_secret, destination_table):
    """Import data from alchemer (surveygizmo) surveys into BigQuery."""
    survey_data = get_survey_data(survey_id, date, api_token, api_secret)
    insert_to_bq(survey_data, destination_table, date)


if __name__ == "__main__":
    main()
