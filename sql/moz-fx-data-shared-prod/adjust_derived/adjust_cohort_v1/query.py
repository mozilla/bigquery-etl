"""Adjust data - download deliverables, clean and upload to BigQuery."""
import csv
import json
import tempfile
from argparse import ArgumentParser

import requests
from google.cloud import bigquery

API_URI = "api.adjust.com"
ENDPOINT = "kpis"
API_VERSION = "v1"

"""The APP_TOKEN_LIST is a list of dictionaries.
Keys are the app_name, and app_token in the form:
{"app_name":"<specific app name from Adjust dash>". "app_token":"<unique Adjust app token>}"
Look here to see the apps we are tracking: https://dash.adjust.com/#/.
It is important to maintain this list in order for the script to work especially in the case of new apps
being added to track in Adjust
"""

CSV_FIELDS = [
    "cohort_start_month",
    "period_length",
    "period",
    "app",
    "app_token",
    "network",
    "network_token",
    "country",
    "os",
    "cohort_size",
    "retained_users",
    "retention_rate",
    "time_spent_per_user",
    "time_spent_per_session",
    "time_spent",
    "sessions_per_user",
    "sessions",
    "date",
]


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


def download_adjust_kpi_data(date, api_token, app_token):
    """Download data from Adjust - API token and APP tokens are called here."""
    start_date = "2020-09-01"  # use date mozilla migrated fennec to fenix as start date
    end_date = date

    kpis = [
        "cohort_size",
        "retained_users",
        "retention_rate",
        "time_spent_per_user",
        "time_spent_per_session",
        "time_spent",
        "sessions_per_user",
        "sessions",
    ]
    groupings = [
        "months",
        "periods",  # from result_parameter
        "apps",
        "networks",
        "countries",
        "os_names",
    ]
    # getting overview metrics for different kpis / Deliverables
    url = f"https://api.adjust.com/kpis/v1/{app_token}/cohorts"  # overview
    url_params = f"start_date={start_date}&end_date={end_date}&kpis={','.join(kpis)}&grouping={','.join(groupings)}"
    headers = {
        "Authorization": f"Bearer {api_token}",
    }

    response = requests.get(url, headers=headers, params=url_params)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)

    return response


def check_json(adjust_response_text):
    """Script will return an empty dictionary for apps on days when there is no data. Check for that here."""
    with tempfile.NamedTemporaryFile() as tmp_json:
        with open(tmp_json.name, "w") as f_json:
            f_json.write(adjust_response_text)
            try:
                query_export = read_json(f_json.name)
            except (
                ValueError
            ):  # ex. json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
                return None
    return query_export


def clean_json(query_export, date_script_run):
    """JSON sometimes has missing keys, need to clean up the data. Cohort_start_date should always be the first of the month. Date or end_date is day script is run.

    https://help.adjust.com/en/article/kpi-glossary#cohort-metrics has descriptions of groupings and metrics.
    """
    period_length = query_export["result_parameters"]["period"]
    run_date = date_script_run

    fields_list = []
    for date in query_export["result_set"]["dates"]:
        r_date = date["date"]
        for period in date["periods"]:
            r_period_length = period_length
            r_period = period.get("period", "no_periods")
            for app in period["apps"]:
                r_app = app.get("name", "no_app_name")
                r_app_token = app.get("token", "no_app_token")
                for network in app["networks"]:
                    r_network = network.get("name", "no_network_name")
                    r_network_token = network.get("token", "no_network_token")
                    for country in network["countries"]:
                        r_country = country["country"]
                        for os_name in country["os_names"]:
                            r_os_name = os_name["os_name"]
                            field_dict = {
                                "cohort_start_month": (r_date),
                                "period_length": (r_period_length),
                                "period": (r_period),
                                "app": (r_app),
                                "app_token": (r_app_token),
                                "network": (r_network),
                                "network_token": (r_network_token),
                                "country": (r_country),
                                "os": (r_os_name),
                                "cohort_size": os_name["kpi_values"][0],
                                "retained_users": os_name["kpi_values"][1],
                                "retention_rate": os_name["kpi_values"][2],
                                "time_spent_per_user": os_name["kpi_values"][3],
                                "time_spent_per_session": os_name["kpi_values"][4],
                                "time_spent": os_name["kpi_values"][5],
                                "sessions_per_user": os_name["kpi_values"][6],
                                "sessions": os_name["kpi_values"][7],
                                "date": run_date,
                            }
                            fields_list.append(field_dict)

    return fields_list


def upload_to_bigquery(csv_data, project, dataset, date):
    """Upload the data to bigquery."""
    print("writing json to csv")

    partition = f"{date}".replace("-", "")

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
                    bigquery.SchemaField("cohort_start_month", "DATE"),
                    bigquery.SchemaField("period_length", "STRING"),
                    bigquery.SchemaField("period", "INTEGER"),
                    bigquery.SchemaField("app", "STRING"),
                    bigquery.SchemaField("app_token", "STRING"),
                    bigquery.SchemaField("network", "STRING"),
                    bigquery.SchemaField("network_token", "STRING"),
                    bigquery.SchemaField("country", "STRING"),
                    bigquery.SchemaField("os", "STRING"),
                    bigquery.SchemaField("cohort_size", "INTEGER"),
                    bigquery.SchemaField("retained_users", "INTEGER"),
                    bigquery.SchemaField("retention_rate", "FLOAT"),
                    bigquery.SchemaField("time_spent_per_user", "INTEGER"),
                    bigquery.SchemaField("time_spent_per_session", "INTEGER"),
                    bigquery.SchemaField("time_spent", "INTEGER"),
                    bigquery.SchemaField("sessions_per_user", "FLOAT"),
                    bigquery.SchemaField("sessions", "INTEGER"),
                    bigquery.SchemaField("date", "DATE"),
                ],
            )
            # Table names are based on the app name seen in the Adjust dashboard"
            destination = f"{project}.{dataset}.adjust_cohort_v1${partition}"

            job = client.load_table_from_file(f_csv, destination, job_config=job_config)

            print(
                f"Writing adjust data for all apps to {destination}. BigQuery job ID: {job.job_id}"
            )
            job.result()


def main():
    """Input data, call functions, get stuff done."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--adjust_api_token", required=True)
    parser.add_argument("--adjust_app_list", required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="adjust_derived")

    args = parser.parse_args()

    app_list = json.loads(args.adjust_app_list)

    data = []

    # Cycle through the apps to get the relevant kpi data
    for app in app_list:
        print(f'This is data for {app["app_name"]}')
        # Ping the Adjust URL and get a response
        json_file = download_adjust_kpi_data(
            args.date,
            args.adjust_api_token,
            app["app_token"],
        )

        query_export = check_json(json_file.text)

        if query_export is not None:
            # This section writes the tmp json data into a temp CSV file which will then be put into a BigQuery table
            adjust_data = clean_json(query_export, args.date)
            data.extend(adjust_data)
        else:
            print(f'no data for {app["app_name"]} today')

    upload_to_bigquery(data, args.project, args.dataset, args.date)


if __name__ == "__main__":
    main()
