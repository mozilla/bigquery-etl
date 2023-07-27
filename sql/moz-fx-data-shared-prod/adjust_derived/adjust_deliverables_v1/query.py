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


def clean_json(query_export):
    """JSON sometimes has missing keys, need to clean up the data."""
    fields_list = []
    for date in query_export["result_set"]["dates"]:
        r_date = date["date"]
        for app in date["apps"]:
            r_app = app.get("name", "no_app_name")
            r_app_token = app.get("token", "no_app_token")
            for network in app["networks"]:
                r_network = network.get("name", "no_network_name")
                r_network_token = network.get("token", "no_network_token")
                for campaign in network["campaigns"]:
                    r_campaign = campaign.get("name", "no_campaign_name")
                    r_campaign_token = campaign.get("token", "no_campaign_token")
                    for adgroup in campaign["adgroups"]:
                        r_adgroup = adgroup.get("name", "no_ad_group_name")
                        r_adgroup_token = adgroup.get("token", "no_adgroup_token")
                        for creative in adgroup["creatives"]:
                            r_creative = creative.get("name", "no_creative_name")
                            r_creative_token = creative.get(
                                "token", "no_creative_token"
                            )
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
                                            "campaign_token": (r_campaign_token),
                                            "adgroup": (r_adgroup),
                                            "adgroup_token": (r_adgroup_token),
                                            "creative": (r_creative),
                                            "creative_token": (r_creative_token),
                                            "country": (r_country),
                                            "os": (r_os_name),
                                            "device": (r_device),
                                            "clicks": device["kpi_values"][0],
                                            "installs": device["kpi_values"][1],
                                            "limit_ad_tracking_install_rate": device[
                                                "kpi_values"
                                            ][2],
                                            "click_conversion_rate": device[
                                                "kpi_values"
                                            ][3],
                                            "impression_conversion_rate": device[
                                                "kpi_values"
                                            ][4],
                                            "sessions": device["kpi_values"][5],
                                            "daus": device["kpi_values"][6],
                                            "waus": device["kpi_values"][7],
                                            "maus": device["kpi_values"][8],
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
            destination = f"{project}.{dataset}.adjust_deliverables_v1${partition}"

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
            args.date, args.adjust_api_token, app["app_token"]
        )

        query_export = check_json(json_file.text)

        if query_export is not None:
            # This section writes the tmp json data into a temp CSV file which will then be put into a BigQuery table
            adjust_data = clean_json(query_export)
            data.extend(adjust_data)
        else:
            print(f'no data for {app["app_name"]} today')

    upload_to_bigquery(data, args.project, args.dataset, args.date)


if __name__ == "__main__":
    main()
