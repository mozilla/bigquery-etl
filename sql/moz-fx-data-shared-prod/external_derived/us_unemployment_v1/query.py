# Import libraries
from datetime import datetime
import pandas as pd
import requests
from argparse import ArgumentParser
import sys
from google.cloud import bigquery
import os


# Define variables
API_KEY = os.getenv("BLS_US_UNEMPLOYMENT_API_KEY")
URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
TIMEOUT_SECONDS = 10
SERIES_ID = "LNS14000000"  # US Unemployment Rate
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
GCS_BUCKET_NO_GS = "moz-fx-data-prod-external-data"
GCS_RESULTS_FPATH = "US_BLS_UNEMPLOYMENT/us_unemployment_%s.csv"
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.us_unemployment_v1"
LOOKBACK_YEARS = 5

# Define functions
def get_start_year(string_date):
    """Input: date as string in YYYY-MM-DD format
    Output: The year = year of the input date - (LOOKBACK_YEARS + 1)"""
    start_year_string = str(
        datetime.strptime(string_date, "%Y-%m-%d").year - (LOOKBACK_YEARS + 1)
    )
    return start_year_string


def get_end_year(string_date):
    """Input: date as string in YYYY-MM-DD format
    Output: The year = year of the input date + 1)"""
    end_year_string = str(datetime.strptime(string_date, "%Y-%m-%d").year + 1)
    return end_year_string


def make_payload(bls_api_key, bls_series_id, start_year, end_year):
    # Generate request payload
    payload = {
        "registrationKey": bls_api_key,
        "seriesid": [bls_series_id],
        "startyear": start_year,
        "endyear": end_year,
    }
    return payload


def get_unemployment_data(api_url, payload, timeout_in_seconds):
    # Make the request
    response = requests.post(api_url, json=payload, timeout=timeout_in_seconds)

    # If it was not successful
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        sys.exit(1)

    # If it was successful
    if response.status_code == 200:
        # Initialize a results dataframe
        results_df = pd.DataFrame(
            {"Year": [], "Period": [], "PeriodName": [], "UnemploymentRate": []}
        )

        # Get the data
        data = response.json()
        series = data.get("Results", {}).get("series", [])

        # Loop through the results
        for s in series:
            for item in s["data"]:
                new_results_df = pd.DataFrame(
                    {
                        "Year": [item["year"]],
                        "Period": [item["period"]],
                        "PeriodName": [item["periodName"]],
                        "UnemploymentRate": [item["value"]],
                    }
                )
                # Append this dataframe to the results dataframe
                results_df = pd.concat([results_df, new_results_df])

    # Write the data to GCS
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    results_df["LastUpdated"] = curr_date

    return results_df


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")

    # Get start year and end year
    start_yr_string = get_start_year(logical_dag_date_string)
    end_yr_string = get_end_year(logical_dag_date_string)

    # Make the request payload
    request_payload = make_payload(API_KEY, SERIES_ID, start_yr_string, end_yr_string)

    # Pull data for this time range
    unemployment_data = get_unemployment_data(URL, request_payload, TIMEOUT_SECONDS)

    # Generate the GCS file path and write to GCS
    curr_date = datetime.today().strftime("%Y-%m-%d")
    final_results_fpath = GCS_BUCKET + GCS_RESULTS_FPATH % (curr_date)
    print("final_results_fpath: ", str(final_results_fpath))
    unemployment_data.to_csv(final_results_fpath, index=False)

    # Write the data from GCS to BQ (using truncate & reload)
    client = bigquery.Client(TARGET_PROJECT)
    load_unemployment_data_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "Year", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Period", "type": "STRING", "mode": "NULLABLE"},
                {"name": "PeriodName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "UnemploymentRate", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "LastUpdated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_unemployment_data_job.result()
