"""Import data from alchemer (surveygizmo) surveys into BigQuery."""

import datetime as dt
import json
import re
from pathlib import Path

import click
import pytz
import requests
from google.cloud import bigquery
from requests import HTTPError


def utc_date_to_eastern_string(date_string: str) -> str:
    """Normalize ISO date to UTC midnight."""
    naive_dt = dt.datetime.strptime(date_string, "%Y-%m-%d")
    as_utc = pytz.utc.localize(naive_dt)
    as_eastern = as_utc.astimezone(pytz.timezone("US/Eastern"))
    return dt.datetime.strftime(as_eastern, "%Y-%m-%d+%H:%M:%S")


def date_plus_one(date_string: str) -> str:
    """Get the day after the current date."""
    return dt.datetime.strftime(
        dt.date.fromisoformat(date_string) + dt.timedelta(days=1), "%Y-%m-%d"
    )


def make_date_iso_compliant(date_string: str) -> str:
    """Convert date to ISO-compliant format."""
    tz_mapping = {"EDT": "-04:00", "EST": "-05:00"}
    tz_match = re.search("E[DS]T", date_string)
    if not tz_match:
        raise ValueError(
            f"Unrecognized timezone in {date_string}. Recognized timezones: {list(tz_mapping.keys())}"
        )

    tz_string = tz_match.group(0)
    tz_repl = tz_mapping[tz_string]
    updated_date_string = re.sub(tz_string, tz_repl, date_string)

    return updated_date_string


def extract_url_variables(survey_response: dict) -> list[dict]:
    """Extract url variables from survey response."""
    vars = survey_response.get("url_variables", {})
    no_url_variables = vars is None or not isinstance(vars, dict)
    if no_url_variables:
        return []

    url_variables = []
    for val in vars.values():
        k, v = val["key"], val["value"]
        url_variable = {"key": k, "value": v}
        url_variables.append(url_variable)

    return url_variables


def extract_survey_data(survey_response: dict) -> list[dict]:
    """Extract survey data from survey response.

    `survey_data` is a dict with question ID as the key and question/response
    details as the value e.g. survey_data: {'25': {'id': 25, 'type': 'RADIO',
    'question': 'I trust Firefox to help me with my online privacy',
    'section_id': 2, 'answer': 'Agree', 'answer_id': 10066, 'shown': True}}
    See https://apihelp.alchemer.com/help/surveyresponse-returned-fields-v5#getobject
    """
    survey_data = []
    for data in survey_response.get("survey_data", {}).values():
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

        survey_data.append(data)

    return survey_data


def format_responses(survey_response: dict, date: str, include_url_variables) -> dict:
    """Return a nested field of responses for each user."""
    survey_data = extract_survey_data(survey_response)
    url_variables = []
    if include_url_variables:
        url_variables = extract_url_variables(survey_response)

    # Note that we are omitted date_submission and date_completed because the
    # timezone is not ISO compliant. The submission date that is passed in as
    # the time parameter should suffice.
    fields = frozenset(
        ["date_started", "id", "language", "response_time", "session_id", "status"]
    )

    _fields = {f: survey_response[f] for f in fields}
    _fields["date_started"] = make_date_iso_compliant(_fields["date_started"])

    response = {
        "submission_date": date,  # this is used as the partitioning field
        "survey_data": survey_data,
        "url_variables": url_variables,
        **_fields,
    }

    return response


def construct_data(
    survey: dict, date: str, include_url_variables: bool = False
) -> list[dict]:
    """Construct response data."""
    return [
        format_responses(resp, date, include_url_variables) for resp in survey["data"]
    ]


def get_survey_data(
    survey_id: str,
    date_string: str,
    token: str,
    secret: str,
    include_url_variables: bool = False,
) -> list[dict]:
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
    resp = _get_request(url)
    survey = resp.json()

    # if the result set is large, we'll have to page through them to get all data
    total_pages = survey.get("total_pages")
    print(f"Found {total_pages} pages after filtering on date={date_string}")

    print("fetching page 1")
    ret = construct_data(survey, date_string, include_url_variables)

    for page in range(2, total_pages + 1):
        print(f"fetching page {page}")
        resp = _get_request(url + f"&page={page}")
        ret = ret + construct_data(resp.json(), date_string, include_url_variables)
    return ret


def _get_request(url: str) -> requests.Response:
    """Make get request and print response if there is an exception."""
    resp = requests.get(url)
    try:
        resp.raise_for_status()
    except HTTPError:
        print(f"Error response: {resp.text}")
        raise
    return resp


def response_schema() -> tuple[bigquery.SchemaField]:
    """Get the schema for the response object from disk."""
    path = Path(__file__).parent / "response.schema.json"
    return bigquery.SchemaField.from_api_repr(
        {"name": "root", "type": "RECORD", "fields": json.loads(path.read_text())}
    ).fields


def insert_to_bq(
    data: list[dict],
    table: str,
    date: str,
    write_disposition: str = bigquery.job.WriteDisposition.WRITE_TRUNCATE,
) -> None:
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
@click.option("--include_url_variables", is_flag=True, default=False)
def main(
    date, survey_id, api_token, api_secret, destination_table, include_url_variables
):
    """Import data from alchemer (surveygizmo) surveys into BigQuery."""
    survey_data = get_survey_data(
        survey_id, date, api_token, api_secret, include_url_variables
    )
    insert_to_bq(survey_data, destination_table, date)


if __name__ == "__main__":
    main()
