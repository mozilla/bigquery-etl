"""Cinder API Addon Moderations Decisions - download decisions, clean and upload to BigQuery."""

import csv
import json
import os
import tempfile
from argparse import ArgumentParser
from time import sleep

import requests
from google.cloud import bigquery

CSV_FIELDS = [
    "user",
    "queue_slug",
    "job_id",
    "uuid",
    "applied_policies",
    "entity",
    "entity_slug",
    "entity_id",
    "created_at",
    "decision_type",
    "job_assigned_at",
    "legacy_decision_labels",
    "policy_map",
    "escalation_details",
]

"""Get the bearer token for Cinder from the environment"""
cinder_bearer_token = os.environ.get("CINDER_TOKEN")


def post_response(url, headers, data):
    """POST response function."""
    response = requests.post(url, headers=headers, data=data)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response


def get_response(url, headers):
    """GET response function."""
    response = requests.get(url, headers=headers)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response


def read_json(filename: str) -> dict:
    """Read JSON file."""
    with open(filename, "r") as f:
        data = json.loads(f.read())
    return data


def write_dict_to_csv(json_data, filename):
    """Write a dictionary to a csv."""
    with open(filename, "w") as out_file:
        dict_writer = csv.DictWriter(out_file, CSV_FIELDS)
        dict_writer.writeheader()
        dict_writer.writerows(json_data)
    # with open(filename) as fp:
    #     reader = csv.reader(fp, delimiter=",", quotechar='"')
    # # next(reader, None)  # skip the headers
    #     data_read = [row for row in reader]
    # print(data_read)


def cinder_addon_decisions_download(date, bearer_token):
    """Download data from Cinder - bearer_token are called here."""
    # getting overview metrics for different kpis / Deliverables
    url = "https://stage.cinder.nonprod.webservices.mozgcp.net/api/v1/decisions/"
    headers = {"accept": "application/json", "authorization": f"Bearer {bearer_token}"}
    print(url)
    response = get_response(url, headers)
    return response


def check_json(cinder_addon_decisions_response_text):
    """Script will return an empty dictionary for apps on days when there is no data. Check for that here."""
    with tempfile.NamedTemporaryFile() as tmp_json:
        with open(tmp_json.name, "w") as f_json:
            f_json.write(cinder_addon_decisions_response_text)
            try:
                query_export = read_json(f_json.name)
            except (
                ValueError
            ):  # ex. json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
                return None
    return query_export


def clean_json(query_export):
    """Turn the json file into a list to be input into a CSV for bq upload."""
    fields_list = []
    for item in query_export["items"]:
        if item.get("user") in (None, ""):
            r_user = "no_user_recorded"
        else:
            r_user = item.get("user", "no_user_recorded")
        if item.get("queue_slug") in (None, ""):
            r_queue_slug = "no_queue_slug_recorded"
        else:
            r_queue_slug = item.get("queue_slug", "no_queue_slug_recorded")
        if item.get("job_id") in (None, ""):
            r_job_id = "no_job_id_recorded"
        else:
            r_job_id = item.get("job_id", "no_job_id_recorded")
        if item.get("uuid") in (None, ""):
            r_uuid = "no_uuid_recorded"
        else:
            r_uuid = item.get("uuid", "no_uuid_recorded")
        if item.get("applied_policies") in (None, ""):
            r_applied_policies = "no_applied_policies_recorded"
        else:
            r_applied_policies = item.get(
                "applied_policies", "no_applied_policies_recorded"
            )
        if item.get("entity") in (None, ""):
            r_entity = "no_entity_recorded"
        else:
            r_entity = item.get("entity", "no_entity_recorded")
        if item.get("entity_slug") in (None, ""):
            r_entity_slug = "no_entity_slug_recorded"
        else:
            r_entity_slug = item.get("entity_slug", "no_entity_slug_recorded")
        if item.get("entity_id") in (None, ""):
            r_entity_id = "no_entity_id_recorded"
        else:
            r_entity_id = item.get("entity_id", "no_entity_id_recorded")
        if item.get("created_at") in (None, ""):
            r_created_at = "no_created_at_recorded"
        else:
            r_created_at = item.get("created_at", "no_created_at_recorded")
        if item.get("decision_type") in (None, ""):
            r_decision_type = "no_decision_type_recorded"
        else:
            r_decision_type = item.get("decision_type", "no_decision_type_recorded")
        if item.get("job_assigned_at") in (None, ""):
            r_job_assigned_at = "no_job_job_assigned_at_recorded"
        else:
            r_job_assigned_at = item.get(
                "job_assigned_at", "no_job_assigned_at_recorded"
            )
        if item.get("typed_metadata") in (None, ""):
            r_legacy_decision_labels = "no_legacy_decision_labels_recorded"
            r_policy_map = "no_policy_map_recorded"
            r_escalation_details = "no_escalation_details_recorded"
        else:
            if item["typed_metadata"]["legacy_decision_labels"] in (None, ""):
                r_legacy_decision_labels = "no_legacy_decision_labels_recorded"
            if item["typed_metadata"]["policy_map"] in (None, ""):
                r_policy_map = "no_policy_map_recorded"
            if item["typed_metadata"]["escalation_details"] in (None, ""):
                r_escalation_details = "no_escalation_details_recorded"
                # r_legacy_decision = metadata.get("legacy_decision_labels", "no_legacy_decision_labels_recorded")
                # r_policy_map = metadata.get("policy_map","no_policy_map_recorded")
                # r_escalation_details = metadata.get("escalation_details", "no_escalation_details_recorded")
        field_dict = {
            "user": r_user,
            "queue_slug": r_queue_slug,
            "job_id": r_job_id,
            "uuid": r_uuid,
            "applied_policies": r_applied_policies,
            "entity": r_entity,
            "entity_slug": r_entity_slug,
            "entity_id": r_entity_id,
            "created_at": r_created_at,
            "decision_type": r_decision_type,
            "job_assigned_at": r_job_assigned_at,
            "legacy_decision_labels": r_legacy_decision_labels,
            "policy_map": r_policy_map,
            "escalation_details": r_escalation_details,
        }
        fields_list.append(field_dict)

    return fields_list


def upload_to_bigquery(csv_data, project, dataset, table_name, date):
    """Upload the data to bigquery."""
    date = date
    print("writing json to csv")
    partition = f"{date}".replace("-", "")
    print(partition)
    with tempfile.NamedTemporaryFile() as tmp_csv:
        with open(tmp_csv.name, "w+b") as f_csv:
            write_dict_to_csv(csv_data, f_csv.name)
            client = bigquery.Client(project)
            job_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date",
                ),
                skip_leading_rows=1,
                schema=[
                    bigquery.SchemaField("date", "DATE"),
                    bigquery.SchemaField("user", "STRING"),
                    bigquery.SchemaField("queue_slug", "STRING"),
                    bigquery.SchemaField("job_id", "STRING"),
                    bigquery.SchemaField("uuid", "STRING"),
                    bigquery.SchemaField("applied_policies", "STRING"),
                    bigquery.SchemaField("entity", "STRING"),
                    bigquery.SchemaField("entity_slug", "STRING"),
                    bigquery.SchemaField("entity_id", "STRING"),
                    bigquery.SchemaField("created_at", "STRING"),
                    bigquery.SchemaField("decision_type", "STRING"),
                    bigquery.SchemaField("job_assigned_at", "STRING"),
                    bigquery.SchemaField("legacy_decision_labels", "STRING"),
                    bigquery.SchemaField("policy_map", "STRING"),
                    bigquery.SchemaField("escalation_details", "STRING"),
                ],
            )
            destination = f"{project}.{dataset}.{table_name}${partition}"
            job = client.load_table_from_file(f_csv, destination, job_config=job_config)
            print(
                f"Writing Decisions data to {destination}. BigQuery job ID: {job.job_id}"
            )
            job.result()


def main():
    """Input data, call functions, get stuff done."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="addon_moderations_derived")

    args = parser.parse_args()

    project = args.project
    dataset = args.dataset
    table_name = "cinder_decisions_raw_v1"

    date = args.date
    bearer_token = cinder_bearer_token

    data = []

    json_file = cinder_addon_decisions_download(date, bearer_token)
    query_export = check_json(json_file.text)

    if query_export is not None:
        # This section writes the tmp json data into a temp CSV file which will then be put into a BigQuery table
        cinder_addon_decisions_data = clean_json(query_export)
        data.extend(cinder_addon_decisions_data)
    else:
        print("no data for today")
        sleep(5)

    upload_to_bigquery(data, project, dataset, table_name, date)


if __name__ == "__main__":
    main()
