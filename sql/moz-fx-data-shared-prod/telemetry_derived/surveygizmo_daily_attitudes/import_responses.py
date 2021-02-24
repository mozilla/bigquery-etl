#!/usr/bin/env python3

"""Import data from daily attitudes heartbeat survey into BigQuery."""

import datetime as dt
import itertools
import re
from argparse import ArgumentParser
from time import sleep

import pytz
import requests
from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)
parser.add_argument("--survey_id", required=True)
parser.add_argument("--sg_api_token", required=True)
parser.add_argument("--sg_api_secret", required=True)
parser.add_argument("--destination_table", default="moz-fx-data-shared-prod.external.survey_gizmo_daily_attitudes")


Q_FIELDS = {
  "I trust Firefox to help me with my online privacy": "trust_firefox",
  "All the sites I’ve visited recently have worked; none of them seem broken": "sites_work",
  "The internet is open and accessible to all": "internet_accessible",
  "Using the internet helped me meet my goals today": "met_goals"
}


def utc_date_to_eastern_string(date_string):
  """Takes in a YYYY-MM-DD date string and returns the equivalent in eastern time of midnight UTC of that date"""
  naive_dt = dt.datetime.strptime(date_string, '%Y-%m-%d')
  as_utc = pytz.utc.localize(naive_dt)
  as_eastern = as_utc.astimezone(pytz.timezone('US/Eastern'))
  return dt.datetime.strftime(as_eastern, '%Y-%m-%d+%H:%M:%S')


def date_plus_one(date_string):
  return dt.datetime.strftime(dt.date.fromisoformat(date_string) + dt.timedelta(days=1), '%Y-%m-%d')


def format_responses(s, date):
    """Takes a single user's responses and returns a list of dictionaries, one per answer,
    formatted to corresponding bigquery fields."""

    # `survey_data` is a dict with question ID as the key and question/response details as the value
    # e.g. survey_data: {'25': {'id': 25, 'type': 'RADIO', 'question': 'I trust Firefox to help me with my online privacy',
    # 'section_id': 2, 'answer': 'Agree', 'answer_id': 10066, 'shown': True}}
    resp = list(s.get("survey_data", {}).values())
    try:
        # Shield ID is sent as a hidden field with question name "Shield ID"
        shield_id = [r.get("answer") for r in resp if r.get('question') == 'Shield ID'][0]
    except IndexError:
        shield_id = None

    return [{
        'shield_id': shield_id,
        'date': date,
        'question': r.get('question'),
        'question_key': Q_FIELDS.get(r.get('question')),
        'value': r.get('answer')
    } for r in resp if r.get('question') != 'Shield ID']


def construct_data(survey, date):
    formatted = [format_responses(resp, date) for resp in survey['data']]
    # flatten list of lists into a single list of dictionaries
    return list(itertools.chain.from_iterable(formatted))


def get_survey_data(survey_id, date_string, token, secret):
    # per SurveyGizmo docs, times are assumed to be eastern
    # https://apihelp.surveygizmo.com/help/filters-v5
    # so UTC midnight must be converted to EST/EDT
    start_date = utc_date_to_eastern_string(date_string)
    end_date = utc_date_to_eastern_string(date_plus_one(date_string))

    url = (f"https://restapi.surveygizmo.com/v5/survey/{survey_id}/surveyresponse"
      f"?api_token={token}&api_token_secret={secret}&results_per_page=500"
      # filter for date_submitted >= start_date
      f"&filter[field][0]=date_submitted&filter[operator][0]=>=&filter[value][0]={start_date}"
      # filter for date_submitted < end_date
      f"&filter[field][1]=date_submitted&filter[operator][1]=<&filter[value][1]={end_date}")
    resp = requests.get(url)
    resp.raise_for_status()
    survey = resp.json()

    # if the result set is large, we'll have to page through them to get all data
    total_pages = survey.get("total_pages")
    print(f"Found {total_pages} pages after filtering on date={date_string}")

    print("fetching page 1")
    ret = construct_data(survey, date_string)

    for page in range(2, total_pages+1):
        print("fetching page {}".format(page))
        resp = requests.get(url + f"&page={page}")
        resp.raise_for_status()
        ret = ret + construct_data(resp.json(), date_string)
    return ret


def insert_to_bq(data, table, date, write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE):
    client = bigquery.Client()
    print(f"Inserting {len(data)} rows into bigquery")
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    partition = f"{table}${date.replace('-', '')}"
    job = client.load_table_from_json(data, partition, job_config=job_config)
    # job.result() returns a LoadJob object if successful, or raises an exception if not
    job.result()


def main():
    args = parser.parse_args()
    survey_data = get_survey_data(args.survey_id, args.date, args.sg_api_token, args.sg_api_secret)
    insert_to_bq(survey_data, args.destination_table, args.date)


if __name__ == "__main__":
    main()
