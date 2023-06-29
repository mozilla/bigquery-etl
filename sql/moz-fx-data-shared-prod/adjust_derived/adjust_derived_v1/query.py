"""Adjust data - download deliverables, clean and upload to BigQuery."""
import csv
import json
import os
import tempfile
from argparse import ArgumentParser

import requests
from google.cloud import bigquery

API_URI = "api.adjust.com"
ENDPOINT = "kpis"
API_VERSION = "v1"

"""This is a list of dictionaries.
Keys are the app_name, and app_token in the form:
{"app_name":"<specific app name from Adjust dash>". "app_token":"<unique Adjust app token>}"
Mozilla has 16 apps on Adjust.
"""

APP_TOKEN_LIST = os.environ.get("ADJUST_APP_TOKEN_LIST")

"""This is Marlene's personal Adjust API Token. We don't have a special service account API Token."""
API_TOKEN = os.environ.get("ADJUST_API_TOKEN")

CSV_FIELDS = []
CSV_FIELDS = [
    "date",
    "app",
    "app_token",
    "network",
    "network_token",
    "campaign",
    "campaign_token",
    "adgroup",
    "adgroup_token",
    "creative",
    "creative_token",
    "country",
    "os",
    "device",
    "clicks",
    "installs",
    "limit_ad_tracking_install_rate",
    "click_conversion_rate",
    "impression_conversion_rate",
    "sessions",
    "daus",
    "waus",
    "maus",
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
    start_date = date
    end_date = date

    kpis = [
        "clicks",
        "installs",
        "limit_ad_tracking_install_rate",
        "click_conversion_rate",
        "impression_conversion_rate",
        "sessions",
        "daus",
        "waus",
        "maus",
    ]
    groupings = [
        "days",
        "apps",
        "networks",
        "campaigns",
        "adgroups",
        "creatives",
        "countries",
        "os_names",
        "device_types",
    ]
    # getting overview metrics for different kpis / Deliverables
    url = f"https://api.adjust.com/kpis/v1/{app_token}"  # overview
    url_params = f"start_date={start_date}&end_date={end_date}&kpis={','.join(kpis)}&grouping={','.join(groupings)}"
    headers = {
        "Authorization": f"Token token={api_token}",
    }

    response = requests.get(url, headers=headers, params=url_params)

    return response


def clean_json(adjust_response_text, adjust_list_part):
    """JSON sometimes has missing keys, need to clean up the data."""
    fields_list = []
    with tempfile.NamedTemporaryFile() as tmp_json:
        with open(tmp_json.name, "w") as f_json:
            f_json.write(adjust_response_text)

            query_export = read_json(f_json.name)

            for date in query_export["result_set"]["dates"]:
                r_date = date["date"]
                for app in date["apps"]:
                    r_app = app["name"]
                    r_app_token = app["token"]
                    for network in app["networks"]:
                        try:
                            r_network = network["name"]
                            r_network_token = network["token"]
                        except KeyError:
                            r_network = "no network name"
                            r_network_token = network["token"]
                        for campaign in network["campaigns"]:
                            try:
                                r_campaign = campaign["name"]
                                r_campaign_token = campaign["token"]
                            except KeyError:
                                r_campaign = "no campaign name"
                                r_campaign_token = campaign["token"]
                            for adgroup in campaign["adgroups"]:
                                try:
                                    r_adgroup = adgroup["name"]
                                    r_adgroup_token = adgroup["token"]
                                except KeyError:
                                    r_adgroup = "no adgroup name"
                                    r_adgroup_token = adgroup["token"]

                                for creative in adgroup["creatives"]:
                                    try:
                                        r_creative = creative["name"]
                                        r_creative_token = creative["token"]
                                    except KeyError:
                                        r_creative = "no creative name"
                                        r_creative_token = creative["token"]
                                    for country in creative["countries"]:
                                        r_country = country["country"]
                                        for os_name in country["os_names"]:
                                            r_os_name = os_name["os_name"]
                                            for device in os_name["device_types"]:
                                                r_device = device["device_type"]
                                                field_dict = {
                                                    "date": (r_date),
                                                    "app": (r_app),
                                                    "app_token": (r_app_token),
                                                    "network": (r_network),
                                                    "network_token": (r_network_token),
                                                    "campaign": (r_campaign),
                                                    "campaign_token": (
                                                        r_campaign_token
                                                    ),
                                                    "adgroup": (r_adgroup),
                                                    "adgroup_token": (r_adgroup_token),
                                                    "creative": (r_creative),
                                                    "creative_token": (
                                                        r_creative_token
                                                    ),
                                                    "country": (r_country),
                                                    "os": (r_os_name),
                                                    "device": (r_device),
                                                    "clicks": device["kpi_values"][0],
                                                    "installs": device["kpi_values"][1],
                                                    "limit_ad_tracking_install_rate": device[
                                                        "kpi_values"
                                                    ][
                                                        2
                                                    ],
                                                    "click_conversion_rate": device[
                                                        "kpi_values"
                                                    ][3],
                                                    "impression_conversion_rate": device[
                                                        "kpi_values"
                                                    ][
                                                        4
                                                    ],
                                                    "sessions": device["kpi_values"][5],
                                                    "daus": device["kpi_values"][6],
                                                    "waus": device["kpi_values"][7],
                                                    "maus": device["kpi_values"][8],
                                                }
                                                fields_list.append(field_dict)
    # print(f'finished filling up list for {adjust_list_part["app_name"]}')
    return fields_list


def upload_to_bigquery(csv_data, project, dataset, adjust_list_part, date):
    """Upload the data to bigquery."""
    print("writing json to csv")

    partition = f"{date}".replace("-", "")

    with tempfile.NamedTemporaryFile() as tmp_csv:
        with open(tmp_csv.name, "w+b") as f_csv:
            write_dict_to_csv(csv_data, f_csv.name)

            client = bigquery.Client(project)

            print("hey, I'm about to config the job\n")

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
                    bigquery.SchemaField("app", "STRING"),
                    bigquery.SchemaField("app_token", "STRING"),
                    bigquery.SchemaField("network", "STRING"),
                    bigquery.SchemaField("network_token", "STRING"),
                    bigquery.SchemaField("campaign", "STRING"),
                    bigquery.SchemaField("campaign_token", "STRING"),
                    bigquery.SchemaField("adgroup", "STRING"),
                    bigquery.SchemaField("adgroup_token", "STRING"),
                    bigquery.SchemaField("creative", "STRING"),
                    bigquery.SchemaField("creative_token", "STRING"),
                    bigquery.SchemaField("country", "STRING"),
                    bigquery.SchemaField("os", "STRING"),
                    bigquery.SchemaField("device", "STRING"),
                    bigquery.SchemaField("clicks", "INTEGER"),
                    bigquery.SchemaField("installs", "INTEGER"),
                    bigquery.SchemaField("limit_ad_tracking_install_rate", "FLOAT"),
                    bigquery.SchemaField("click_conversion_rate", "FLOAT"),
                    bigquery.SchemaField("impression_conversion_rate", "FLOAT"),
                    bigquery.SchemaField("sessions", "INTEGER"),
                    bigquery.SchemaField("daus", "INTEGER"),
                    bigquery.SchemaField("waus", "INTEGER"),
                    bigquery.SchemaField("maus", "INTEGER"),
                ],
            )

            # Table names are based on the app name seen in the Adjust dashboard"
            destination = f'{project}.{dataset}.adjust_deliverables_{adjust_list_part["app_name"]}_v1_${partition}'
            print(destination)

            job = client.load_table_from_file(f_csv, destination, job_config=job_config)

            print(f"Running job {job.job_id}")
            job.result()


def main():
    """Input data, call functions, get stuff done."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="analysis")

    args = parser.parse_args()

    # print("Something might be happening")

    app_list = json.loads(APP_TOKEN_LIST)

    # Cycle through the apps to get the relevant kpi data
    for num in range(len(app_list)):
        print(
            f'This is data for {app_list[num]["app_name"]} - {app_list[num]["app_token"]}'
        )

        # Ping the Adjust URL and get a response
        json_file = download_adjust_kpi_data(
            args.date, API_TOKEN, app_list[num]["app_token"]
        )

        """ Gather the data and clean where necessary. Some campaigns, adgroups and creative
            are missing keys such as 'name'. This next section cleans that up and stores the
            results into a tmp file """

        data = []
        if json_file.text:
            data = clean_json(json_file.text, app_list[num])
            print("finished json_read")
        else:
            print(f'no data for {app_list[num]["app_name"]} today')
            continue

        """ This section writes the tmp json data into a temp CSV file which will then
            be put into a BigQuery table"""
        if data:
            upload_to_bigquery(
                data, args.project, args.dataset, app_list[num], args.date
            )
        else:
            print("no data to upload")


if __name__ == "__main__":
    main()
