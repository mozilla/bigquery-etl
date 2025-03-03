# Load libraries
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import bigquery

# Set variables
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.google_play_store_diagnostic_data_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "GOOGLE_PLAY_STORE/google_play_store_diagnostic_data_%s.csv"

metrics_of_interest = {'MetricSet1': 'QueryString1',
                       'MetricSet2': 'QueryString2'}

#Get the token
bearer_token = os.getenv("GOOGLE_PLAY_STORE_DEVELOPER_CREDS")

def fetch_data(url, hdr, pyld, timeout_limit):
    """Inputs: URL, Header, Payload, Timeout Limit
    Output: Shows errors if errors arise during the fetch"""
    try:
        response = requests.get(url, headers=hdr, data=pyld, timeout=timeout_limit)
        response.raise_for_status()  # Raises an HTTPError for 4xx and 5xx status codes
        return response.json()  # or response.text if expecting plain text
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors (4xx and 5xx)
        raise  # Re-raise the error if needed
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server.")
        raise
    except requests.exceptions.Timeout:
        print("Error: The request timed out.")
        raise
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}")
        raise

# Define function to pull data from the Google Play Store Developer API
def pull_play_store_data(metric_set, query_string):
    """ 
    Inputs:
    * metric_set - STRING
    * query_set - STRING
    Outputs:
    * Dataframe with data
    """
    url = f"https://playdeveloperreporting.googleapis.com/v1beta1/apps/com.example.app/{metric_set}:{query_string}"
    response = requests.get(api_url, timeout=10)
    headers={"Authorization": f"Bearer {bearer_token}"}
    payload = {}
    results = fetch_data(url, hdr=headers, pyld=payload, timeout_limit=10)

    play_store_data = results.json()
    print('play_store_data')
    print(play_store_data)

    #COnvert data to a dataframe
    play_store_data_df = pd.DataFrame(play_store_data)
    return play_store_data_df




if __name__ == "__main__":


    #Loop through metrics of interest
    for metric_string, query_string in metrics_of_interest.items():
        

      #Get play store data
      play_store_df = pull_play_store_data()

    #Calculate current date and append to the data
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)
    play_store_df['last_updated'] = curr_date


    #Get the GCS file path & save to GCS
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (curr_date)
    print("final_results_fpath: ", final_results_fpath)
    play_store_df.to_csv(final_results_fpath, index=False)


    # Load to BQ  - write/truncate
    client = bigquery.Client(TARGET_PROJECT)
    load_csv_to_gcp_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "gdp_country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gdp_country_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gdp_country_code_iso3", "type": "STRING", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gcp_job.result()


def main():
    """Call the API, save data to GCS, """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_date_date_string = logical_dag_date.strftime("%Y-%m-%d")

if __name__ == "__main__":
    main()